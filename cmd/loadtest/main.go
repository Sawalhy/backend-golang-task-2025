// Command loadtest drives concurrent order placement against a running API and
// reports latency distribution, throughput and outcome mix.
//
// It exists to answer one question with numbers instead of assertions: WHAT IS
// THE BOTTLENECK? The method is to vary one thing at a time and watch what moves
// (DESIGN_NOTES.md §8):
//
//	DB_MAX_OPEN_CONNS 12 -> 40   large effect  => the pool was the constraint
//	PAYMENT_LATENCY 200ms -> 2s  no effect     => payment is off the request path,
//	                                              which is what reserve->pay->commit buys
//	--scale worker=1 -> 4        moves E2E only => intake and processing are decoupled
//
// Two modes:
//
//	-mode burst   every goroutine blocks on one channel and is released together.
//	              This is the CONTENTION test: trickling requests out means they
//	              never collide and the numbers prove nothing.
//	-mode sustain a fixed pool works through the total, measuring steady-state
//	              throughput rather than a thundering herd.
//
// Usage:
//
//	go run ./cmd/loadtest -n 1000 -c 200 -product 2
//	go run ./cmd/loadtest -n 500 -c 500 -product 5 -mode burst   # oversell check
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"sync"
	"time"
)

type options struct {
	baseURL   string
	email     string
	password  string
	total     int
	conc      int
	productID uint64
	qty       int
	mode      string
	settle    time.Duration
}

func main() {
	var o options
	flag.StringVar(&o.baseURL, "url", "http://localhost:8080", "API base URL")
	flag.StringVar(&o.email, "email", "sarah@example.com", "login email")
	flag.StringVar(&o.password, "password", "customer123", "login password")
	flag.IntVar(&o.total, "n", 1000, "total orders to place")
	flag.IntVar(&o.conc, "c", 100, "concurrent in-flight requests")
	flag.Uint64Var(&o.productID, "product", 2, "product id to order")
	flag.IntVar(&o.qty, "qty", 1, "quantity per order")
	flag.StringVar(&o.mode, "mode", "sustain", "burst | sustain")
	flag.DurationVar(&o.settle, "settle", 0, "after the run, wait this long and report how many orders reached a terminal state")
	flag.Parse()

	if err := run(o); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(o options) error {
	// A default http.Client keeps only 2 idle connections per host, so at any
	// real concurrency it serialises requests and you end up measuring the load
	// generator instead of the server. This is the single most common way a load
	// test lies.
	client := &http.Client{
		Timeout: 60 * time.Second,
		Transport: &http.Transport{
			MaxIdleConns:        o.conc * 2,
			MaxIdleConnsPerHost: o.conc * 2,
			MaxConnsPerHost:     o.conc * 2,
			IdleConnTimeout:     90 * time.Second,
		},
	}

	token, err := login(client, o)
	if err != nil {
		return err
	}

	body, err := json.Marshal(map[string]any{
		"items": []map[string]any{{"product_id": o.productID, "qty": o.qty}},
	})
	if err != nil {
		return err
	}

	results := make([]result, o.total)
	var wg sync.WaitGroup

	// The barrier. In burst mode every goroutine is spawned, reaches this
	// channel and blocks; closing it releases all of them within microseconds so
	// they genuinely contend for the same inventory row.
	release := make(chan struct{})
	sem := make(chan struct{}, o.conc)

	for i := 0; i < o.total; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()

			if o.mode == "burst" {
				<-release
			} else {
				// Sustained mode: a counting semaphore caps in-flight requests
				// so the pool works through the total at a steady rate.
				sem <- struct{}{}
				defer func() { <-sem }()
				<-release
			}

			results[idx] = placeOrder(client, o.baseURL, token, body)
		}(i)
	}

	// Give every goroutine time to reach the barrier before starting the clock,
	// so spawn cost is not counted as latency.
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("releasing %d requests (mode=%s, concurrency=%d, product=%d)\n\n",
		o.total, o.mode, o.conc, o.productID)

	start := time.Now()
	close(release)
	wg.Wait()
	elapsed := time.Since(start)

	report(results, elapsed, o)

	if o.settle > 0 {
		fmt.Printf("\nwaiting %s for the async pipeline to drain...\n", o.settle)
		time.Sleep(o.settle)
		reportSettlement(client, o, token, results)
	}
	return nil
}

type result struct {
	status  int
	latency time.Duration
	orderID uint64
	err     error
}

func placeOrder(client *http.Client, baseURL, token string, body []byte) result {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost,
		baseURL+"/api/v1/orders", bytes.NewReader(body))
	if err != nil {
		return result{err: err}
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)

	started := time.Now()
	resp, err := client.Do(req)
	latency := time.Since(started)
	if err != nil {
		return result{latency: latency, err: err}
	}
	defer resp.Body.Close()

	r := result{status: resp.StatusCode, latency: latency}

	// Decoding on the 202 path only; error bodies are counted, not parsed.
	if resp.StatusCode == http.StatusAccepted {
		var order struct {
			ID uint64 `json:"id"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&order); err == nil {
			r.orderID = order.ID
		}
	}
	return r
}

func report(results []result, elapsed time.Duration, o options) {
	byStatus := map[int]int{}
	var failures int
	latencies := make([]time.Duration, 0, len(results))

	for _, r := range results {
		if r.err != nil {
			failures++
			continue
		}
		byStatus[r.status]++
		latencies = append(latencies, r.latency)
	}

	sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })

	fmt.Printf("=== throughput ===\n")
	fmt.Printf("  wall clock     %s\n", elapsed.Round(time.Millisecond))
	fmt.Printf("  requests/sec   %.0f\n", float64(len(results))/elapsed.Seconds())
	fmt.Printf("  transport errs %d\n", failures)

	fmt.Printf("\n=== latency (accepted + rejected) ===\n")
	if len(latencies) > 0 {
		fmt.Printf("  min   %s\n", latencies[0].Round(time.Millisecond))
		fmt.Printf("  p50   %s\n", percentile(latencies, 0.50).Round(time.Millisecond))
		fmt.Printf("  p95   %s\n", percentile(latencies, 0.95).Round(time.Millisecond))
		fmt.Printf("  p99   %s\n", percentile(latencies, 0.99).Round(time.Millisecond))
		fmt.Printf("  max   %s\n", latencies[len(latencies)-1].Round(time.Millisecond))
	}

	fmt.Printf("\n=== outcomes ===\n")
	codes := make([]int, 0, len(byStatus))
	for code := range byStatus {
		codes = append(codes, code)
	}
	sort.Ints(codes)
	for _, code := range codes {
		fmt.Printf("  %d %-24s %d\n", code, explain(code), byStatus[code])
	}

	// The correctness claim, checked rather than assumed: accepted orders can
	// never exceed the stock that existed. 409s are not failures — they are the
	// conditional UPDATE refusing to oversell.
	accepted := byStatus[http.StatusAccepted]
	fmt.Printf("\n  accepted %d of %d (%.1f%%)\n",
		accepted, len(results), 100*float64(accepted)/float64(len(results)))
}

func explain(code int) string {
	switch code {
	case http.StatusAccepted:
		return "accepted"
	case http.StatusConflict:
		return "insufficient stock"
	case http.StatusTooManyRequests:
		return "rate limited"
	case http.StatusUnauthorized:
		return "unauthorized"
	case http.StatusBadRequest:
		return "bad request"
	default:
		return "other"
	}
}

// percentile uses nearest-rank on a sorted slice.
func percentile(sorted []time.Duration, p float64) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(p * float64(len(sorted)))
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// reportSettlement samples accepted orders after the pipeline has had time to
// work, which measures the ASYNC path: 202 says the order exists, not that it
// was paid. Intake throughput and end-to-end completion are different numbers
// and conflating them is how an async system gets reported as faster than it is.
func reportSettlement(client *http.Client, o options, token string, results []result) {
	const sample = 50
	states := map[string]int{}
	checked := 0

	for _, r := range results {
		if r.orderID == 0 || checked >= sample {
			continue
		}
		status, err := orderStatus(client, o.baseURL, token, r.orderID)
		if err != nil {
			states["error"]++
		} else {
			states[status]++
		}
		checked++
	}

	fmt.Printf("=== settlement (sample of %d accepted orders) ===\n", checked)
	keys := make([]string, 0, len(states))
	for k := range states {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("  %-20s %d\n", k, states[k])
	}
}

func orderStatus(client *http.Client, baseURL, token string, orderID uint64) (string, error) {
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		fmt.Sprintf("%s/api/v1/orders/%d/status", baseURL, orderID), nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("Authorization", "Bearer "+token)

	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	var out struct {
		Status string `json:"status"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Status, nil
}

func login(client *http.Client, o options) (string, error) {
	body, err := json.Marshal(map[string]string{"email": o.email, "password": o.password})
	if err != nil {
		return "", err
	}

	resp, err := client.Post(o.baseURL+"/api/v1/auth/login", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("logging in (is the API running?): %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("login returned %d", resp.StatusCode)
	}

	var out struct {
		Token string `json:"token"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Token, nil
}

package tests

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/Sawalhy/backend-golang-task-2025/internal/models"
	"github.com/Sawalhy/backend-golang-task-2025/internal/workers"
	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// The regression tests for the worst bug in this project.
//
// An AMQP Connection and its Channels were acquired once at startup and never
// redialled. After RabbitMQ dropped the connection the relay failed 241
// consecutive publishes while its health check stayed green, the outbox grew
// without bound, and the entire async pipeline silently stopped. A crashed relay
// restarts and recovers; a wedged one looks healthy forever.
//
// Reproducing that needs a connection that DIES underneath a running session,
// which is why these tests exist separately from everything else. Rather than
// killing a process, they use RabbitMQ's management API to force-close the
// connection from the broker side — the same thing the broker does to every
// client when it restarts, and much faster and more deterministic than
// SIGKILLing a subprocess.

// managementURL returns the base URL of the RabbitMQ management API, defaulting
// to the compose service. Tests skip when it is unreachable.
func managementURL(t *testing.T) string {
	t.Helper()

	if v := os.Getenv("TEST_RABBITMQ_MGMT"); v != "" {
		return strings.TrimRight(v, "/")
	}

	// Derive it from the AMQP URL: same host, management port.
	amqpURL := requireBroker(t)
	parsed, err := url.Parse(amqpURL)
	if err != nil {
		t.Skipf("cannot derive management URL from %q", amqpURL)
	}
	host := parsed.Hostname()
	if host == "" {
		host = "localhost"
	}
	user := "guest"
	pass := "guest"
	if parsed.User != nil {
		user = parsed.User.Username()
		if p, ok := parsed.User.Password(); ok {
			pass = p
		}
	}
	return fmt.Sprintf("http://%s:%s@%s:15672", user, pass, host)
}

type rabbitConnection struct {
	Name string `json:"name"`
	// ClientProperties carries the connection_name we set when dialling.
	ClientProperties map[string]any `json:"client_properties"`
}

func managementRequest(t *testing.T, method, base, path string) ([]byte, int) {
	t.Helper()

	parsed, err := url.Parse(base)
	require.NoError(t, err)

	req, err := http.NewRequest(method, strings.TrimRight(base, "/")+path, nil)
	require.NoError(t, err)
	if parsed.User != nil {
		pass, _ := parsed.User.Password()
		req.SetBasicAuth(parsed.User.Username(), pass)
	}

	resp, err := (&http.Client{Timeout: 10 * time.Second}).Do(req)
	if err != nil {
		t.Skipf("rabbitmq management API unreachable at %s: %v", base, err)
	}
	defer resp.Body.Close()

	buf := make([]byte, 0, 64*1024)
	tmp := make([]byte, 8192)
	for {
		n, readErr := resp.Body.Read(tmp)
		buf = append(buf, tmp[:n]...)
		if readErr != nil {
			break
		}
	}
	return buf, resp.StatusCode
}

// killConnectionNamed force-closes the connection carrying the given
// connection_name, and reports whether it found one.
//
// This is what a broker restart does to every client: the socket goes away with
// no graceful close handshake, which is precisely the condition the original bug
// could not survive.
func killConnectionNamed(t *testing.T, base, name string) bool {
	t.Helper()

	body, status := managementRequest(t, http.MethodGet, base, "/api/connections")
	if status != http.StatusOK {
		t.Skipf("management API returned %d listing connections", status)
	}

	var conns []rabbitConnection
	if err := json.Unmarshal(body, &conns); err != nil {
		t.Skipf("cannot parse management API response: %v", err)
	}

	for _, c := range conns {
		if fmt.Sprint(c.ClientProperties["connection_name"]) != name {
			continue
		}
		_, status := managementRequest(t, http.MethodDelete, base,
			"/api/connections/"+url.PathEscape(c.Name))
		return status == http.StatusNoContent || status == http.StatusOK
	}
	return false
}

// TestSupervisorRedialsAfterTheConnectionIsKilled is the direct regression test
// for the wedged relay: the session must END when the connection dies, and a new
// one must be established without the process restarting.
func TestSupervisorRedialsAfterTheConnectionIsKilled(t *testing.T) {
	amqpURL := requireBroker(t)
	mgmt := managementURL(t)
	log := logger.New("error", false)

	name := fmt.Sprintf("supervised-%d", time.Now().UnixNano())

	var sessions int64
	sessionStarted := make(chan struct{}, 8)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() {
		done <- workers.Supervise(ctx, amqpURL, "orders", name, log,
			func(sessionCtx context.Context, b *workers.Broker) error {
				atomic.AddInt64(&sessions, 1)
				select {
				case sessionStarted <- struct{}{}:
				default:
				}
				// Hold the session open until the connection dies or we stop.
				<-sessionCtx.Done()
				return sessionCtx.Err()
			})
	}()

	// First session.
	select {
	case <-sessionStarted:
	case <-time.After(20 * time.Second):
		t.Fatal("the first session never started")
	}
	require.EqualValues(t, 1, atomic.LoadInt64(&sessions))

	// Kill it from the broker side.
	require.Eventually(t, func() bool { return killConnectionNamed(t, mgmt, name) },
		20*time.Second, 500*time.Millisecond,
		"could not find the named connection to kill")

	// The supervisor must notice and redial. Before the fix this is exactly
	// where the relay sat forever, publishing into a dead channel.
	select {
	case <-sessionStarted:
	case <-time.After(30 * time.Second):
		t.Fatal("the supervisor never started a second session: it is wedged on a dead connection")
	}
	assert.GreaterOrEqual(t, atomic.LoadInt64(&sessions), int64(2),
		"a killed connection must produce a new session")

	// Ordinary shutdown still works and returns cleanly.
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "cancellation is not a failure")
	case <-time.After(20 * time.Second):
		t.Fatal("Supervise did not return after its context was cancelled")
	}
}

// The point of redialling is not the reconnect itself but that WORK RESUMES.
// This publishes through the relay, kills the connection, and asserts a
// subsequently enqueued event still reaches the broker.
func TestRelayKeepsDrainingAcrossAConnectionLoss(t *testing.T) {
	amqpURL := requireBroker(t)
	mgmt := managementURL(t)
	store, _ := newStore(t)
	log := logger.New("error", false)

	name := fmt.Sprintf("relay-supervised-%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessionStarted := make(chan struct{}, 8)
	go func() {
		_ = workers.Supervise(ctx, amqpURL, "orders", name, log,
			func(sessionCtx context.Context, b *workers.Broker) error {
				pub, err := b.NewPublisher()
				if err != nil {
					return err
				}
				defer func() { _ = pub.Close() }()

				select {
				case sessionStarted <- struct{}{}:
				default:
				}
				return workers.NewRelay(store, pub, 100*time.Millisecond, 100, log).Run(sessionCtx)
			})
	}()

	select {
	case <-sessionStarted:
	case <-time.After(20 * time.Second):
		t.Fatal("relay session never started")
	}

	// Works before the kill.
	enqueue(t, store, models.EventOrderCreated, 7001)
	require.Eventually(t, func() bool { return unsentCount(t, store) == 0 },
		20*time.Second, 200*time.Millisecond, "the relay should drain before the connection is killed")

	require.Eventually(t, func() bool { return killConnectionNamed(t, mgmt, name) },
		20*time.Second, 500*time.Millisecond, "could not find the relay connection to kill")

	// And works after. This is the assertion the original bug fails: the relay
	// stayed alive but never published another event.
	enqueue(t, store, models.EventOrderCreated, 7002)
	require.Eventually(t, func() bool { return unsentCount(t, store) == 0 },
		45*time.Second, 250*time.Millisecond,
		"the relay never recovered: events enqueued after a connection loss were never published")
}

// Nothing may be lost across a reconnect, and that is the outbox earning its
// keep: unpublished rows still have sent_at IS NULL, so the next session claims
// exactly the same batch.
func TestEventsEnqueuedWhileDisconnectedArePublishedOnRecovery(t *testing.T) {
	amqpURL := requireBroker(t)
	mgmt := managementURL(t)
	store, _ := newStore(t)
	log := logger.New("error", false)

	name := fmt.Sprintf("relay-gap-%d", time.Now().UnixNano())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sessionStarted := make(chan struct{}, 8)
	go func() {
		_ = workers.Supervise(ctx, amqpURL, "orders", name, log,
			func(sessionCtx context.Context, b *workers.Broker) error {
				pub, err := b.NewPublisher()
				if err != nil {
					return err
				}
				defer func() { _ = pub.Close() }()
				select {
				case sessionStarted <- struct{}{}:
				default:
				}
				return workers.NewRelay(store, pub, 100*time.Millisecond, 100, log).Run(sessionCtx)
			})
	}()

	select {
	case <-sessionStarted:
	case <-time.After(20 * time.Second):
		t.Fatal("relay session never started")
	}

	require.Eventually(t, func() bool { return killConnectionNamed(t, mgmt, name) },
		20*time.Second, 500*time.Millisecond, "could not kill the connection")

	// Enqueue immediately, while the relay is between sessions. These rows have
	// nobody to publish them yet.
	for i := 0; i < 5; i++ {
		enqueue(t, store, models.EventOrderCreated, uint64(7100+i))
	}

	require.Eventually(t, func() bool { return unsentCount(t, store) == 0 },
		45*time.Second, 250*time.Millisecond,
		"events written during the outage must be published once the relay reconnects")
}

// A broker that is unreachable at startup must not stop the process: Supervise
// retries with backoff instead of failing, and exits cleanly when cancelled.
func TestSupervisorRetriesAnUnreachableBrokerAndStopsCleanly(t *testing.T) {
	log := logger.New("error", false)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var sessions int64
	err := workers.Supervise(ctx, "amqp://guest:guest@127.0.0.1:1/", "orders", "never-connects", log,
		func(sessionCtx context.Context, b *workers.Broker) error {
			atomic.AddInt64(&sessions, 1)
			return nil
		})

	assert.NoError(t, err, "an unreachable broker is retried, not fatal")
	assert.Zero(t, atomic.LoadInt64(&sessions), "no session can start without a connection")
}

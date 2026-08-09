package middleware

import (
	"context"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These run against a REAL Redis. The whole point of the limiter is that the
// read-modify-write is atomic inside Redis, and a fake client would execute the
// Lua script in Go — testing the opposite of what matters.
func requireRedis(t *testing.T) *redis.Client {
	t.Helper()

	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping rate limiter tests")
	}

	rdb := redis.NewClient(&redis.Options{Addr: addr})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("redis at %s unreachable: %v", addr, err)
	}

	t.Cleanup(func() { _ = rdb.Close() })
	return rdb
}

// uniqueKey keeps tests from sharing a bucket, since Redis outlives the process.
func uniqueKey(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test:%s:%d", t.Name(), rand.Int63())
}

func TestTokenBucketAllowsBurstThenRejects(t *testing.T) {
	rdb := requireRedis(t)
	ctx := context.Background()

	// rate 1/s so refill is negligible across the loop; burst 5 is the capacity.
	limiter := NewRateLimiter(rdb, 1, 5)
	key := uniqueKey(t)

	for i := 1; i <= 5; i++ {
		allowed, remaining, err := limiter.Allow(ctx, key)
		require.NoError(t, err)
		assert.True(t, allowed, "request %d is within the burst", i)
		assert.Equal(t, 5-i, remaining)
	}

	allowed, _, err := limiter.Allow(ctx, key)
	require.NoError(t, err)
	assert.False(t, allowed, "the bucket is empty, so the 6th must be refused")
}

// The bucket refills continuously rather than resetting on a boundary. A fixed
// window would let 100 requests through at 11:59:59 and 100 more at 12:00:00 and
// call it "100 per minute"; a token bucket smooths instead of stepping.
func TestTokenBucketRefillsOverTime(t *testing.T) {
	rdb := requireRedis(t)
	ctx := context.Background()

	limiter := NewRateLimiter(rdb, 10, 2) // 10 tokens/sec, capacity 2
	key := uniqueKey(t)

	for i := 0; i < 2; i++ {
		allowed, _, err := limiter.Allow(ctx, key)
		require.NoError(t, err)
		require.True(t, allowed)
	}

	allowed, _, err := limiter.Allow(ctx, key)
	require.NoError(t, err)
	require.False(t, allowed, "drained")

	// At 10/sec, 300ms is worth ~3 tokens, capped at the burst of 2.
	time.Sleep(300 * time.Millisecond)

	allowed, _, err = limiter.Allow(ctx, key)
	require.NoError(t, err)
	assert.True(t, allowed, "tokens must accrue with elapsed time")
}

// THE test for this component, and the reason the bucket lives in Lua.
//
// The naive limiter does GET, compute, SET from the application. With N replicas
// hammering one key those three steps interleave and the limit leaks badly under
// exactly the load it exists to control — the same shape as SELECT-then-UPDATE
// overselling stock. Redis runs a script to completion with nothing interleaved,
// so 200 concurrent callers against a burst of 20 must yield exactly 20.
func TestTokenBucketIsAtomicUnderConcurrency(t *testing.T) {
	rdb := requireRedis(t)
	ctx := context.Background()

	const (
		callers = 200
		burst   = 20
	)
	// rate 1/s so refill during the burst is negligible and the expected count
	// is exact rather than approximate.
	limiter := NewRateLimiter(rdb, 1, burst)
	key := uniqueKey(t)

	var (
		wg      sync.WaitGroup
		release = make(chan struct{})
		results = make([]bool, callers)
		errs    = make([]error, callers)
	)

	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-release // released together, so they genuinely contend
			results[idx], _, errs[idx] = limiter.Allow(ctx, key)
		}(i)
	}
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()

	granted := 0
	for i, allowed := range results {
		require.NoError(t, errs[i])
		if allowed {
			granted++
		}
	}

	assert.Equal(t, burst, granted,
		"exactly the burst may be granted; more means the read-modify-write leaked")
}

// A limiter is a guard rail, not the service. If Redis is unreachable, rejecting
// every request turns a cache outage into a full outage — so it fails OPEN.
//
// This is the behaviour SOLUTION.md claims, asserted directly rather than
// assumed. The trade is that an attacker who can take Redis down also removes
// the limit, which is the lesser problem and is why the failure is logged loudly.
func TestRateLimiterFailsOpenWhenRedisIsUnreachable(t *testing.T) {
	gin.SetMode(gin.TestMode)

	// Nothing listens here.
	dead := redis.NewClient(&redis.Options{
		Addr:        "127.0.0.1:1",
		DialTimeout: 200 * time.Millisecond,
		ReadTimeout: 200 * time.Millisecond,
	})
	defer func() { _ = dead.Close() }()

	limiter := NewRateLimiter(dead, 1, 1)

	// Allow reports the failure to its caller...
	allowed, _, err := limiter.Allow(context.Background(), uniqueKey(t))
	require.Error(t, err, "the caller must be able to see that Redis is down")
	assert.False(t, allowed)

	// ...but the middleware must still let the request through.
	r := gin.New()
	r.Use(limiter.Middleware())
	r.GET("/x", func(c *gin.Context) { c.String(http.StatusOK, "served") })

	for i := 0; i < 5; i++ {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/x", nil))
		require.Equal(t, http.StatusOK, w.Code,
			"a Redis outage must not become an API outage")
		assert.Equal(t, "served", w.Body.String())
	}
}

// Buckets are per identity. If they were shared, one noisy client behind a NAT
// would lock out everyone else on that address.
func TestBucketsAreIsolatedPerKey(t *testing.T) {
	rdb := requireRedis(t)
	ctx := context.Background()

	limiter := NewRateLimiter(rdb, 1, 1)
	noisy, quiet := uniqueKey(t), uniqueKey(t)

	allowed, _, err := limiter.Allow(ctx, noisy)
	require.NoError(t, err)
	require.True(t, allowed)

	allowed, _, err = limiter.Allow(ctx, noisy)
	require.NoError(t, err)
	require.False(t, allowed, "the noisy client exhausted its own bucket")

	allowed, _, err = limiter.Allow(ctx, quiet)
	require.NoError(t, err)
	assert.True(t, allowed, "a different key must be unaffected")
}

// 429 with the headers a well-behaved client needs to back off, rather than
// leaving it to guess and retry immediately.
func TestMiddlewareReturns429WithHeaders(t *testing.T) {
	rdb := requireRedis(t)
	gin.SetMode(gin.TestMode)

	limiter := NewRateLimiter(rdb, 1, 2)

	// Pin ONE user id for the whole test. Generating it inside the handler would
	// give every request its own bucket, and nothing would ever be limited.
	userID := uint64(rand.Int63n(1_000_000) + 1)

	r := gin.New()
	r.Use(func(c *gin.Context) {
		c.Set(CtxUserID, userID)
		c.Next()
	}, limiter.Middleware())
	r.GET("/x", func(c *gin.Context) { c.String(http.StatusOK, "ok") })

	var last *httptest.ResponseRecorder
	for i := 0; i < 3; i++ {
		last = httptest.NewRecorder()
		r.ServeHTTP(last, httptest.NewRequest(http.MethodGet, "/x", nil))
	}

	require.Equal(t, http.StatusTooManyRequests, last.Code)
	assert.Equal(t, "2", last.Header().Get("X-RateLimit-Limit"))
	assert.NotEmpty(t, last.Header().Get("Retry-After"), "tell the client when to come back")

	remaining := last.Header().Get("X-RateLimit-Remaining")
	n, err := strconv.Atoi(remaining)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, n, 0, "remaining must never be reported negative")
}

// Idle buckets expire so abandoned keys do not accumulate in Redis forever.
func TestIdleBucketsExpire(t *testing.T) {
	rdb := requireRedis(t)
	ctx := context.Background()

	limiter := NewRateLimiter(rdb, 10, 10)
	key := uniqueKey(t)

	_, _, err := limiter.Allow(ctx, key)
	require.NoError(t, err)

	ttl, err := rdb.TTL(ctx, "ratelimit:"+key).Result()
	require.NoError(t, err)
	assert.Positive(t, ttl, "the bucket must carry a TTL")
	// A full refill takes burst/rate seconds; after that the key is
	// indistinguishable from a fresh one, so keeping it is pointless.
	assert.LessOrEqual(t, ttl, 5*time.Second)
}

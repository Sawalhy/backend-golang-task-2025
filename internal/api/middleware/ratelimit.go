package middleware

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"

	"github.com/Sawalhy/backend-golang-task-2025/pkg/logger"
)

// tokenBucket is a rate limiter that runs entirely inside Redis.
//
// The Lua matters. The naive limiter does GET, compute, SET from Go — three
// round trips with a gap between the read and the write, and with N API replicas
// hammering the same key those gaps interleave and the limit leaks badly under
// exactly the load it exists to control. Redis executes a script atomically, so
// the whole read-modify-write is one indivisible step. It is the same
// compare-and-swap discipline the database code uses, in a different store.
//
// A token bucket rather than a fixed window because a fixed window allows a
// double burst across the boundary: 100 requests at 11:59:59 and 100 more at
// 12:00:00 both pass a "100 per minute" window. The bucket refills continuously,
// so it smooths instead of stepping — and `burst` still allows a legitimate
// short spike, which is usually what real clients do.
const tokenBucketScript = `
local key       = KEYS[1]
local rate      = tonumber(ARGV[1])   -- tokens added per second
local burst     = tonumber(ARGV[2])   -- bucket capacity
local now       = tonumber(ARGV[3])   -- unix seconds, fractional
local requested = tonumber(ARGV[4])

local bucket = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(bucket[1])
local ts     = tonumber(bucket[2])

if tokens == nil then
  tokens = burst
  ts     = now
end

-- Refill for the time elapsed since the last request, capped at capacity.
local elapsed = math.max(0, now - ts)
tokens = math.min(burst, tokens + (elapsed * rate))

local allowed = 0
if tokens >= requested then
  tokens  = tokens - requested
  allowed = 1
end

redis.call('HMSET', key, 'tokens', tokens, 'ts', now)
-- Expire idle buckets so abandoned keys do not accumulate forever. A full
-- refill takes burst/rate seconds; after that the key is indistinguishable
-- from a fresh one, so keeping it is pointless.
redis.call('EXPIRE', key, math.ceil(burst / rate) + 1)

return {allowed, math.floor(tokens)}
`

type RateLimiter struct {
	rdb    *redis.Client
	script *redis.Script
	rate   int
	burst  int
}

func NewRateLimiter(rdb *redis.Client, rps, burst int) *RateLimiter {
	if rps <= 0 {
		rps = 50
	}
	if burst <= 0 {
		burst = rps * 2
	}
	return &RateLimiter{
		rdb:    rdb,
		script: redis.NewScript(tokenBucketScript),
		rate:   rps,
		burst:  burst,
	}
}

// Allow reports whether one request may proceed, and how many tokens remain.
func (l *RateLimiter) Allow(ctx context.Context, key string) (bool, int, error) {
	now := float64(time.Now().UnixNano()) / float64(time.Second)

	res, err := l.script.Run(ctx, l.rdb, []string{"ratelimit:" + key},
		l.rate, l.burst, now, 1).Slice()
	if err != nil {
		return false, 0, fmt.Errorf("running rate limit script: %w", err)
	}
	if len(res) != 2 {
		return false, 0, fmt.Errorf("rate limit script returned %d values, want 2", len(res))
	}

	allowed, _ := res[0].(int64)
	remaining, _ := res[1].(int64)
	return allowed == 1, int(remaining), nil
}

// Middleware limits per authenticated user, falling back to client IP for
// anonymous routes — otherwise every unauthenticated caller would share one
// bucket and a single noisy client could lock out login for everyone.
func (l *RateLimiter) Middleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		key := "ip:" + c.ClientIP()
		if uid := UserID(c); uid != 0 {
			key = "user:" + strconv.FormatUint(uid, 10)
		}

		allowed, remaining, err := l.Allow(c.Request.Context(), key)
		if err != nil {
			// Fail OPEN. A limiter is a guard rail, not the service: if Redis is
			// unreachable, rejecting every request converts a cache outage into a
			// full outage. The trade is that an attacker who can take Redis down
			// also removes the limit, which is the lesser problem and is why the
			// failure is logged loudly rather than swallowed.
			logger.FromContext(c.Request.Context()).Error("rate limiter unavailable, allowing request",
				"error", err)
			c.Next()
			return
		}

		c.Header("X-RateLimit-Limit", strconv.Itoa(l.burst))
		c.Header("X-RateLimit-Remaining", strconv.Itoa(remaining))

		if !allowed {
			// Retry-After tells a well-behaved client when to come back, instead
			// of leaving it to guess and retry immediately.
			c.Header("Retry-After", strconv.Itoa(1))
			c.AbortWithStatusJSON(http.StatusTooManyRequests, gin.H{
				"error":    "rate limit exceeded",
				"trace_id": c.GetString(CtxTraceID),
			})
			return
		}

		c.Next()
	}
}

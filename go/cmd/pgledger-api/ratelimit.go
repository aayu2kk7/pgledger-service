package main

import (
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

const maxConfiguredRateLimit = 1_000_000

const (
	writeRatePerMinute = 120
	writeBurst         = 30
	limiterEntryTTL    = 10 * time.Minute
	maxLimiterEntries  = 10_000
)

type rateLimitEntry struct {
	tokens    float64
	updatedAt time.Time
	lastSeen  time.Time
}

type writeRateLimiter struct {
	mu      sync.Mutex
	entries map[string]rateLimitEntry
	limit   float64
	burst   float64
	now     func() time.Time
}

func newWriteRateLimiter(perMinute, burst int) *writeRateLimiter {
	return &writeRateLimiter{
		entries: make(map[string]rateLimitEntry),
		limit:   float64(perMinute) / 60,
		burst:   float64(burst),
		now:     time.Now,
	}
}

func writeRateLimitConfigFromEnv() (int, int, error) {
	perMinute, err := positiveEnvInt("PGLEDGER_WRITE_RATE_LIMIT_PER_MINUTE", writeRatePerMinute)
	if err != nil {
		return 0, 0, err
	}
	burst, err := positiveEnvInt("PGLEDGER_WRITE_RATE_LIMIT_BURST", writeBurst)
	if err != nil {
		return 0, 0, err
	}
	return perMinute, burst, nil
}

func positiveEnvInt(name string, fallback int) (int, error) {
	value := strings.TrimSpace(os.Getenv(name))
	if value == "" {
		return fallback, nil
	}
	parsed, err := strconv.Atoi(value)
	if err != nil || parsed <= 0 || parsed > maxConfiguredRateLimit {
		return 0, fmt.Errorf("%s must be an integer between 1 and %d", name, maxConfiguredRateLimit)
	}
	return parsed, nil
}

func (limiter *writeRateLimiter) allow(client string) (bool, time.Duration) {
	now := limiter.now()
	limiter.mu.Lock()
	defer limiter.mu.Unlock()

	entry, exists := limiter.entries[client]
	if !exists {
		if len(limiter.entries) >= maxLimiterEntries {
			for key, candidate := range limiter.entries {
				if now.Sub(candidate.lastSeen) > limiterEntryTTL {
					delete(limiter.entries, key)
				}
			}
			if len(limiter.entries) >= maxLimiterEntries {
				return false, time.Minute
			}
		}
		entry = rateLimitEntry{tokens: limiter.burst, updatedAt: now}
	}
	entry.tokens += now.Sub(entry.updatedAt).Seconds() * limiter.limit
	if entry.tokens > limiter.burst {
		entry.tokens = limiter.burst
	}
	entry.updatedAt = now
	entry.lastSeen = now
	if entry.tokens >= 1 {
		entry.tokens--
		limiter.entries[client] = entry
		return true, 0
	}

	limiter.entries[client] = entry
	if len(limiter.entries) > maxLimiterEntries || len(limiter.entries)%256 == 0 {
		for key, candidate := range limiter.entries {
			if now.Sub(candidate.lastSeen) > limiterEntryTTL {
				delete(limiter.entries, key)
			}
		}
	}
	wait := time.Duration((1 - entry.tokens) / limiter.limit * float64(time.Second))
	wait = max(wait, time.Second)
	return false, wait
}

func writeRateLimitMiddleware(next http.Handler, limiter *writeRateLimiter) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || (r.URL.Path != "/v1/accounts" && r.URL.Path != "/v1/transfers") {
			next.ServeHTTP(w, r)
			return
		}

		allowed, retryAfter := limiter.allow(clientIP(r))
		if !allowed {
			w.Header().Set("Retry-After", strconv.Itoa(int(retryAfter.Seconds()+0.5)))
			writeError(w, http.StatusTooManyRequests, "write request rate limit exceeded; retry later")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func clientIP(r *http.Request) string {
	// Render sets the original client address as the first X-Forwarded-For value.
	// Ignore malformed values and fall back to the direct peer address.
	for candidate := range strings.SplitSeq(r.Header.Get("X-Forwarded-For"), ",") {
		if ip := net.ParseIP(strings.TrimSpace(candidate)); ip != nil {
			return ip.String()
		}
	}
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		if ip := net.ParseIP(host); ip != nil {
			return ip.String()
		}
	}
	return r.RemoteAddr
}

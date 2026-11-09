package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestWriteRateLimitMiddleware(t *testing.T) {
	limiter := newWriteRateLimiter(60, 2)
	var handled int
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		handled++
		w.WriteHeader(http.StatusNoContent)
	})
	handler := writeRateLimitMiddleware(next, limiter)

	request := func(path, forwardedFor string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(http.MethodPost, path, nil)
		r.RemoteAddr = "192.0.2.10:1234"
		if forwardedFor != "" {
			r.Header.Set("X-Forwarded-For", forwardedFor)
		}
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, r)
		return response
	}

	if got := request("/v1/transfers", "203.0.113.1").Code; got != http.StatusNoContent {
		t.Fatalf("first write: expected 204, got %d", got)
	}
	if got := request("/v1/accounts", "203.0.113.1").Code; got != http.StatusNoContent {
		t.Fatalf("second write: expected 204, got %d", got)
	}
	limited := request("/v1/transfers", "203.0.113.1")
	if limited.Code != http.StatusTooManyRequests {
		t.Fatalf("third write: expected 429, got %d", limited.Code)
	}
	if limited.Header().Get("Retry-After") == "" {
		t.Fatal("expected Retry-After header")
	}
	if got := request("/v1/transfers", "203.0.113.2").Code; got != http.StatusNoContent {
		t.Fatalf("different client: expected 204, got %d", got)
	}
	readRequest := httptest.NewRequest(http.MethodGet, "/v1/accounts", nil)
	readResponse := httptest.NewRecorder()
	handler.ServeHTTP(readResponse, readRequest)
	if readResponse.Code != http.StatusNoContent {
		t.Fatalf("GET request should not be rate limited, got %d", readResponse.Code)
	}
	if handled != 4 {
		t.Fatalf("expected 4 requests to reach next handler, got %d", handled)
	}
}

func TestWriteRateLimiterRefillsTokens(t *testing.T) {
	limiter := newWriteRateLimiter(60, 1)
	now := time.Now()
	limiter.now = func() time.Time { return now }
	if allowed, _ := limiter.allow("client"); !allowed {
		t.Fatal("initial burst token should be allowed")
	}
	if allowed, _ := limiter.allow("client"); allowed {
		t.Fatal("request without a token should be rejected")
	}
	now = now.Add(time.Second)
	if allowed, _ := limiter.allow("client"); !allowed {
		t.Fatal("refilled token should be allowed")
	}
}

func TestWriteRateLimitRunsBeforeAuthentication(t *testing.T) {
	limiter := newWriteRateLimiter(60, 1)
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := writeRateLimitMiddleware(authMiddleware(next, "user", "password"), limiter)

	request := httptest.NewRequest(http.MethodPost, "/v1/transfers", nil)
	request.RemoteAddr = "192.0.2.10:1234"
	first := httptest.NewRecorder()
	handler.ServeHTTP(first, request)
	if first.Code != http.StatusUnauthorized {
		t.Fatalf("first unauthenticated write: expected 401, got %d", first.Code)
	}

	request = httptest.NewRequest(http.MethodPost, "/v1/transfers", nil)
	request.RemoteAddr = "192.0.2.10:5678"
	second := httptest.NewRecorder()
	handler.ServeHTTP(second, request)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second unauthenticated write: expected 429, got %d", second.Code)
	}
}

func TestAuthMiddleware(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := authMiddleware(next, "demo-user", "demo-password")

	t.Run("requires credentials", func(t *testing.T) {
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, response.Code)
		}
		if response.Header().Get("WWW-Authenticate") == "" {
			t.Error("expected Basic Auth challenge header")
		}
	})

	t.Run("rejects incorrect credentials", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.SetBasicAuth("demo-user", "wrong-password")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusUnauthorized {
			t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, response.Code)
		}
	})

	t.Run("accepts correct credentials", func(t *testing.T) {
		request := httptest.NewRequest(http.MethodGet, "/", nil)
		request.SetBasicAuth("demo-user", "demo-password")
		response := httptest.NewRecorder()
		handler.ServeHTTP(response, request)
		if response.Code != http.StatusNoContent {
			t.Fatalf("expected status %d, got %d", http.StatusNoContent, response.Code)
		}
	})
}

func TestAuthMiddlewareDisabledWithoutCredentials(t *testing.T) {
	next := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	})
	handler := authMiddleware(next, "", "")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusNoContent {
		t.Fatalf("expected status %d, got %d", http.StatusNoContent, response.Code)
	}
}

func TestValidateServerConfig(t *testing.T) {
	cases := []struct {
		name        string
		requireAuth bool
		username    string
		password    string
		databaseURL string
		wantError   bool
	}{
		{name: "local defaults", databaseURL: ""},
		{name: "optional credentials", username: "demo", password: "secret"},
		{name: "requires paired credentials", username: "demo", wantError: true},
		{name: "required auth needs credentials", requireAuth: true, databaseURL: "postgres://demo", wantError: true},
		{name: "required auth needs explicit database URL", requireAuth: true, username: "demo", password: "secret", wantError: true},
		{name: "protected deployment needs TLS", requireAuth: true, username: "demo", password: "secret", databaseURL: "postgres://demo", wantError: true},
		{name: "valid protected deployment", requireAuth: true, username: "demo", password: "secret", databaseURL: "postgres://demo?sslmode=require"},
	}
	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			err := validateServerConfig(testCase.requireAuth, testCase.username, testCase.password, testCase.databaseURL)
			if (err != nil) != testCase.wantError {
				t.Fatalf("expected error=%t, got %v", testCase.wantError, err)
			}
		})
	}
}

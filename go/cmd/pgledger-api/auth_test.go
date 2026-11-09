package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

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

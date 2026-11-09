package main

import (
	"crypto/subtle"
	"errors"
	"net/http"
	"net/url"
)

func validateServerConfig(requireAuth bool, username, password, databaseURL string) error {
	if (username == "") != (password == "") {
		return errors.New("PGLEDGER_HTTP_USER and PGLEDGER_HTTP_PASSWORD must be set together")
	}
	if requireAuth && username == "" {
		return errors.New("PGLEDGER_REQUIRE_AUTH=true requires PGLEDGER_HTTP_USER and PGLEDGER_HTTP_PASSWORD")
	}
	if requireAuth && databaseURL == "" {
		return errors.New("PGLEDGER_REQUIRE_AUTH=true requires an explicit DATABASE_URL")
	}
	if requireAuth && !databaseRequiresTLS(databaseURL) {
		return errors.New("protected deployments require DATABASE_URL with sslmode=require, verify-ca, or verify-full")
	}
	return nil
}

func databaseRequiresTLS(databaseURL string) bool {
	parsed, err := url.Parse(databaseURL)
	if err != nil {
		return false
	}
	switch parsed.Query().Get("sslmode") {
	case "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func authMiddleware(next http.Handler, username, password string) http.Handler {
	if username == "" && password == "" {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		providedUser, providedPassword, ok := r.BasicAuth()
		if !ok || subtle.ConstantTimeCompare([]byte(providedUser), []byte(username)) != 1 ||
			subtle.ConstantTimeCompare([]byte(providedPassword), []byte(password)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="pgledger", charset="UTF-8"`)
			writeError(w, http.StatusUnauthorized, "authentication required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

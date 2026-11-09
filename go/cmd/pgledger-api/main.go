package main

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/internal/dburl"
)

//go:embed web/*
var webFiles embed.FS

type server struct{ db *pgxpool.Pool }

const (
	maxAccountNameLength = 200
	maxCurrencyLength    = 20
	maxIdempotencyLength = 255
)

type accountRequest struct {
	Name                 string          `json:"name"`
	Currency             string          `json:"currency"`
	AllowNegativeBalance *bool           `json:"allow_negative_balance,omitempty"`
	AllowPositiveBalance *bool           `json:"allow_positive_balance,omitempty"`
	Metadata             json.RawMessage `json:"metadata,omitempty"`
}

type transferRequest struct {
	FromAccountID string          `json:"from_account_id"`
	ToAccountID   string          `json:"to_account_id"`
	Amount        string          `json:"amount"`
	EventAt       *time.Time      `json:"event_at,omitempty"`
	Metadata      json.RawMessage `json:"metadata,omitempty"`
}

type account struct {
	ID                   string          `json:"id"`
	Name                 string          `json:"name"`
	Currency             string          `json:"currency"`
	Balance              string          `json:"balance"`
	Version              int64           `json:"version"`
	AllowNegativeBalance bool            `json:"allow_negative_balance"`
	AllowPositiveBalance bool            `json:"allow_positive_balance"`
	Metadata             json.RawMessage `json:"metadata"`
	CreatedAt            time.Time       `json:"created_at"`
	UpdatedAt            time.Time       `json:"updated_at"`
}

type transfer struct {
	ID            string          `json:"id"`
	FromAccountID string          `json:"from_account_id"`
	ToAccountID   string          `json:"to_account_id"`
	Amount        string          `json:"amount"`
	CreatedAt     time.Time       `json:"created_at"`
	EventAt       time.Time       `json:"event_at"`
	Metadata      json.RawMessage `json:"metadata"`
}

type entry struct {
	ID                     string          `json:"id"`
	AccountID              string          `json:"account_id"`
	TransferID             string          `json:"transfer_id"`
	Amount                 string          `json:"amount"`
	AccountPreviousBalance string          `json:"account_previous_balance"`
	AccountCurrentBalance  string          `json:"account_current_balance"`
	AccountVersion         int64           `json:"account_version"`
	CreatedAt              time.Time       `json:"created_at"`
	EventAt                time.Time       `json:"event_at"`
	Metadata               json.RawMessage `json:"metadata"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	databaseURL := os.Getenv("DATABASE_URL")
	requireAuth := strings.EqualFold(os.Getenv("PGLEDGER_REQUIRE_AUTH"), "true")
	authUser := os.Getenv("PGLEDGER_HTTP_USER")
	authPassword := os.Getenv("PGLEDGER_HTTP_PASSWORD")
	if err := validateServerConfig(requireAuth, authUser, authPassword, databaseURL); err != nil {
		log.Fatal(err)
	}
	if databaseURL == "" {
		databaseURL = dburl.URL()
	}
	db, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatalf("connect to PostgreSQL: %v", err)
	}
	defer db.Close()
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("ping PostgreSQL: %v", err)
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		if port := os.Getenv("PORT"); port != "" {
			addr = net.JoinHostPort("0.0.0.0", port)
		} else {
			addr = "127.0.0.1:8080"
		}
	}
	httpServer := &http.Server{
		Addr: addr,
		Handler: writeRateLimitMiddleware(
			authMiddleware(newHandler(db), authUser, authPassword),
			newWriteRateLimiter(writeRatePerMinute, writeBurst),
		),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      30 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    32 << 10,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("HTTP shutdown: %v", err)
		}
	}()

	log.Printf("pgledger API listening on %s", addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("HTTP server: %v", err)
	}
}

func newHandler(db *pgxpool.Pool) http.Handler {
	api := &server{db: db}
	mux := http.NewServeMux()
	staticFiles, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	mux.Handle("GET /", http.FileServer(http.FS(staticFiles)))
	mux.HandleFunc("GET /healthz", api.health)
	mux.HandleFunc("GET /v1/accounts", api.getAccounts)
	mux.HandleFunc("POST /v1/accounts", api.createAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", api.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/entries", api.getAccountEntries)
	mux.HandleFunc("POST /v1/transfers", api.createTransfer)
	return mux
}

func (s *server) health(w http.ResponseWriter, r *http.Request) {
	if err := s.db.Ping(r.Context()); err != nil {
		writeError(w, http.StatusServiceUnavailable, "database unavailable")
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) createAccount(w http.ResponseWriter, r *http.Request) {
	var request accountRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if strings.TrimSpace(request.Name) == "" || strings.TrimSpace(request.Currency) == "" {
		writeError(w, http.StatusBadRequest, "name and currency are required")
		return
	}
	if len(request.Name) > maxAccountNameLength || len(request.Currency) > maxCurrencyLength {
		writeError(w, http.StatusBadRequest, "name or currency exceeds the maximum length")
		return
	}
	allowNegative, allowPositive := true, true
	if request.AllowNegativeBalance != nil {
		allowNegative = *request.AllowNegativeBalance
	}
	if request.AllowPositiveBalance != nil {
		allowPositive = *request.AllowPositiveBalance
	}
	var result account
	err := s.db.QueryRow(r.Context(), `
		SELECT id, name, currency, balance::text, version, allow_negative_balance,
		allow_positive_balance, metadata, created_at, updated_at
		FROM pgledger_create_account($1, $2, $3, $4, $5::jsonb)`,
		request.Name, request.Currency, allowNegative, allowPositive, nullableJSON(request.Metadata)).Scan(
		&result.ID, &result.Name, &result.Currency, &result.Balance, &result.Version,
		&result.AllowNegativeBalance, &result.AllowPositiveBalance, &result.Metadata,
		&result.CreatedAt, &result.UpdatedAt,
	)
	if err != nil {
		writeDatabaseError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func (s *server) getAccount(w http.ResponseWriter, r *http.Request) {
	var result account
	err := s.db.QueryRow(r.Context(), `
		SELECT id, name, currency, balance::text, version, allow_negative_balance,
		allow_positive_balance, metadata, created_at, updated_at
		FROM pgledger_accounts_view
		WHERE id = $1`, r.PathValue("id")).Scan(
		&result.ID, &result.Name, &result.Currency, &result.Balance, &result.Version,
		&result.AllowNegativeBalance, &result.AllowPositiveBalance, &result.Metadata,
		&result.CreatedAt, &result.UpdatedAt,
	)
	if errors.Is(err, pgx.ErrNoRows) {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}
	if err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (s *server) getAccounts(w http.ResponseWriter, r *http.Request) {
	limit, offset, err := parsePage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	rows, err := s.db.Query(r.Context(), `
		SELECT a.id, a.name, a.currency, a.balance::text, a.version, a.allow_negative_balance,
			a.allow_positive_balance, a.metadata, a.created_at, a.updated_at
		FROM pgledger_accounts_view a
		LEFT JOIN LATERAL (
			SELECT MAX(created_at) AS last_transfer_at
			FROM pgledger_transfers_view
			WHERE from_account_id = a.id OR to_account_id = a.id
		) activity ON TRUE
		ORDER BY GREATEST(COALESCE(activity.last_transfer_at, a.created_at), a.created_at) DESC,
			a.created_at DESC, a.id DESC
		LIMIT $1 OFFSET $2`, limit, offset)
	if err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	defer rows.Close()

	results := make([]account, 0)
	for rows.Next() {
		var result account
		if err := rows.Scan(
			&result.ID, &result.Name, &result.Currency, &result.Balance, &result.Version,
			&result.AllowNegativeBalance, &result.AllowPositiveBalance, &result.Metadata,
			&result.CreatedAt, &result.UpdatedAt,
		); err != nil {
			writeDatabaseUnavailable(w, err)
			return
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *server) getAccountEntries(w http.ResponseWriter, r *http.Request) {
	accountID := r.PathValue("id")
	limit, offset, err := parsePage(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}
	var exists bool
	if err := s.db.QueryRow(r.Context(), "SELECT EXISTS (SELECT 1 FROM pgledger_accounts_view WHERE id = $1)", accountID).Scan(&exists); err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	if !exists {
		writeError(w, http.StatusNotFound, "account not found")
		return
	}

	rows, err := s.db.Query(r.Context(), `
		SELECT id, account_id, transfer_id, amount::text, account_previous_balance::text,
		account_current_balance::text, account_version, created_at, event_at, metadata
		FROM pgledger_entries_view
		WHERE account_id = $1
		ORDER BY account_version DESC
		LIMIT $2 OFFSET $3`, accountID, limit, offset)
	if err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	defer rows.Close()

	results := make([]entry, 0)
	for rows.Next() {
		var result entry
		if err := rows.Scan(
			&result.ID, &result.AccountID, &result.TransferID, &result.Amount,
			&result.AccountPreviousBalance, &result.AccountCurrentBalance,
			&result.AccountVersion, &result.CreatedAt, &result.EventAt, &result.Metadata,
		); err != nil {
			writeDatabaseUnavailable(w, err)
			return
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		writeDatabaseUnavailable(w, err)
		return
	}
	for left, right := 0, len(results)-1; left < right; left, right = left+1, right-1 {
		results[left], results[right] = results[right], results[left]
	}
	writeJSON(w, http.StatusOK, results)
}

func (s *server) createTransfer(w http.ResponseWriter, r *http.Request) {
	var request transferRequest
	if !decodeJSON(w, r, &request) {
		return
	}
	if request.FromAccountID == "" || request.ToAccountID == "" || request.Amount == "" {
		writeError(w, http.StatusBadRequest, "from_account_id, to_account_id, and amount are required")
		return
	}
	key := r.Header.Get("Idempotency-Key")
	if strings.TrimSpace(key) == "" {
		writeError(w, http.StatusBadRequest, "Idempotency-Key is required")
		return
	}
	if len(key) > maxIdempotencyLength {
		writeError(w, http.StatusBadRequest, "Idempotency-Key must be at most 255 bytes")
		return
	}
	var eventAt *time.Time
	if request.EventAt != nil {
		eventAt = request.EventAt
	}
	var result transfer
	err := s.db.QueryRow(r.Context(), `
		SELECT id, from_account_id, to_account_id, amount::text, created_at, event_at, metadata
		FROM pgledger_create_transfer($1, $2, $3::numeric, $4, $5::jsonb, $6)`,
		request.FromAccountID, request.ToAccountID, request.Amount, eventAt,
		nullableJSON(request.Metadata), nullableString(key),
	).Scan(&result.ID, &result.FromAccountID, &result.ToAccountID, &result.Amount,
		&result.CreatedAt, &result.EventAt, &result.Metadata)
	if err != nil {
		writeDatabaseError(w, err)
		return
	}
	writeJSON(w, http.StatusCreated, result)
}

func decodeJSON(w http.ResponseWriter, r *http.Request, destination any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(destination); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON request")
		return false
	}
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "request must contain one JSON object")
		return false
	}
	return true
}

func nullableJSON(value json.RawMessage) any {
	if len(value) == 0 || string(value) == "null" {
		return nil
	}
	return string(value)
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func writeDatabaseError(w http.ResponseWriter, err error) {
	// Keep PostgreSQL and driver details out of the response while retaining
	// them in server logs.
	log.Printf("ledger request failed: %v", err)
	message := "ledger rejected the request"
	var postgresError *pgconn.PgError
	if errors.As(err, &postgresError) && postgresError.Code == "P0001" {
		switch {
		case strings.Contains(postgresError.Message, "does not allow negative balance"):
			message = "Transfer would overdraw an account that disallows negative balances."
		case strings.Contains(postgresError.Message, "does not allow positive balance"):
			message = "Transfer would credit an account that disallows positive balances."
		case strings.Contains(postgresError.Message, "Idempotency key"):
			message = "This idempotency key was already used for a different request."
		case strings.Contains(postgresError.Message, "Amount"):
			message = "Amount must be a positive finite decimal."
		}
	}
	writeError(w, http.StatusUnprocessableEntity, message)
}

func writeDatabaseUnavailable(w http.ResponseWriter, err error) {
	log.Printf("ledger query failed: %v", err)
	writeError(w, http.StatusServiceUnavailable, "database unavailable")
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Printf("write response: %v", err)
	}
}

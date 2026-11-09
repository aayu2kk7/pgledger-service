package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/testhelpers"
)

func TestCreateAccountHTTP(t *testing.T) {
	db := testDatabase(t)
	handler := newHandler(db)

	request := httptest.NewRequest(http.MethodPost, "/v1/accounts", strings.NewReader(
		`{"name":"api-test-account","currency":"USD","allow_negative_balance":false,"metadata":{"source":"test"}}`,
	))
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)

	if response.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, response.Code, response.Body.String())
	}
	var result account
	if err := json.NewDecoder(response.Body).Decode(&result); err != nil {
		t.Fatalf("decode account response: %v", err)
	}
	if !strings.HasPrefix(result.ID, "pgla_") {
		t.Errorf("expected pgla_ account ID, got %q", result.ID)
	}
	if result.Name != "api-test-account" || result.Currency != "USD" {
		t.Errorf("unexpected account response: %+v", result)
	}
	if result.AllowNegativeBalance {
		t.Error("expected allow_negative_balance to be false")
	}
	if string(result.Metadata) != `{"source": "test"}` && string(result.Metadata) != `{"source":"test"}` {
		t.Errorf("unexpected metadata: %s", result.Metadata)
	}
}

func TestCreateTransferHTTPIdempotency(t *testing.T) {
	db := testDatabase(t)
	handler := newHandler(db)
	fromAccount := testhelpers.CreateAccount(t, db, "api-test-from", "USD")
	toAccount := testhelpers.CreateAccount(t, db, "api-test-to", "USD")
	key := fmt.Sprintf("api-test-%d-%d", time.Now().UnixNano(), time.Now().Unix())
	body, err := json.Marshal(transferRequest{
		FromAccountID: fromAccount.ID,
		ToAccountID:   toAccount.ID,
		Amount:        "12.34",
	})
	if err != nil {
		t.Fatal(err)
	}

	first := transferRequestHTTP(t, handler, body, key)
	if first.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s", http.StatusCreated, first.Code, first.Body.String())
	}
	firstTransfer := decodeTransferResponse(t, first.Body)
	if firstTransfer.Amount != "12.34" {
		t.Errorf("expected amount 12.34, got %q", firstTransfer.Amount)
	}

	replay := transferRequestHTTP(t, handler, body, key)
	if replay.Code != http.StatusCreated {
		t.Fatalf("expected replay status %d, got %d: %s", http.StatusCreated, replay.Code, replay.Body.String())
	}
	replayedTransfer := decodeTransferResponse(t, replay.Body)
	if replayedTransfer.ID != firstTransfer.ID {
		t.Errorf("expected replayed ID %q, got %q", firstTransfer.ID, replayedTransfer.ID)
	}

	differentBody, err := json.Marshal(transferRequest{
		FromAccountID: fromAccount.ID,
		ToAccountID:   toAccount.ID,
		Amount:        "99.99",
	})
	if err != nil {
		t.Fatal(err)
	}
	mismatch := transferRequestHTTP(t, handler, differentBody, key)
	if mismatch.Code != http.StatusUnprocessableEntity {
		t.Errorf("expected idempotency mismatch status %d, got %d: %s", http.StatusUnprocessableEntity, mismatch.Code, mismatch.Body.String())
	}

	var transferCount int
	if err := db.QueryRow(t.Context(), "SELECT count(*) FROM pgledger_transfers_view WHERE from_account_id = $1", fromAccount.ID).Scan(&transferCount); err != nil {
		t.Fatal(err)
	}
	if transferCount != 1 {
		t.Errorf("expected exactly one transfer after retry and mismatch, got %d", transferCount)
	}
	var fromBalance, toBalance string
	if err := db.QueryRow(t.Context(), "SELECT balance::text FROM pgledger_accounts_view WHERE id = $1", fromAccount.ID).Scan(&fromBalance); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(t.Context(), "SELECT balance::text FROM pgledger_accounts_view WHERE id = $1", toAccount.ID).Scan(&toBalance); err != nil {
		t.Fatal(err)
	}
	if fromBalance != "-12.34" || toBalance != "12.34" {
		t.Errorf("unexpected balances after idempotent transfer: from=%s to=%s", fromBalance, toBalance)
	}

	accountResponse := getHTTP(handler, "/v1/accounts/"+toAccount.ID)
	if accountResponse.Code != http.StatusOK {
		t.Fatalf("expected account read status %d, got %d: %s", http.StatusOK, accountResponse.Code, accountResponse.Body.String())
	}
	var foundAccount account
	if err := json.NewDecoder(accountResponse.Body).Decode(&foundAccount); err != nil {
		t.Fatalf("decode account read response: %v", err)
	}
	if foundAccount.Balance != "12.34" || foundAccount.Version != 1 {
		t.Errorf("unexpected account state from API: balance=%s version=%d", foundAccount.Balance, foundAccount.Version)
	}

	entriesResponse := getHTTP(handler, "/v1/accounts/"+toAccount.ID+"/entries")
	if entriesResponse.Code != http.StatusOK {
		t.Fatalf("expected entries read status %d, got %d: %s", http.StatusOK, entriesResponse.Code, entriesResponse.Body.String())
	}
	var foundEntries []entry
	if err := json.NewDecoder(entriesResponse.Body).Decode(&foundEntries); err != nil {
		t.Fatalf("decode entries response: %v", err)
	}
	if len(foundEntries) != 1 {
		t.Fatalf("expected one account entry, got %d", len(foundEntries))
	}
	if foundEntries[0].TransferID != firstTransfer.ID || foundEntries[0].AccountCurrentBalance != "12.34" {
		t.Errorf("unexpected account entry: %+v", foundEntries[0])
	}
}

func TestCreateTransferRequiresIdempotencyKeyHTTP(t *testing.T) {
	handler := newHandler(nil)
	body, err := json.Marshal(transferRequest{
		FromAccountID: "pgla_source",
		ToAccountID:   "pgla_destination",
		Amount:        "1.00",
	})
	if err != nil {
		t.Fatal(err)
	}
	response := transferRequestHTTP(t, handler, body, "")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected missing key status %d, got %d: %s", http.StatusBadRequest, response.Code, response.Body.String())
	}
	var responseBody map[string]string
	if err := json.NewDecoder(response.Body).Decode(&responseBody); err != nil {
		t.Fatal(err)
	}
	if responseBody["error"] != "Idempotency-Key is required" {
		t.Errorf("unexpected missing-key error: %q", responseBody["error"])
	}
}

func TestConcurrentSameIdempotencyKeyCreatesOneTransferHTTP(t *testing.T) {
	db := testDatabase(t)
	fromAccount := testhelpers.CreateAccount(t, db, "api-test-idempotent-race-from", "USD")
	toAccount := testhelpers.CreateAccount(t, db, "api-test-idempotent-race-to", "USD")
	body, err := json.Marshal(transferRequest{
		FromAccountID: fromAccount.ID,
		ToAccountID:   toAccount.ID,
		Amount:        "7.25",
	})
	if err != nil {
		t.Fatal(err)
	}
	key := fmt.Sprintf("api-test-idempotent-race-%d", time.Now().UnixNano())
	server := httptest.NewServer(newHandler(db))
	defer server.Close()

	const requestCount = 10
	type result struct {
		status int
		id     string
		err    error
	}
	results := make(chan result, requestCount)
	start := make(chan struct{})
	var wait sync.WaitGroup
	client := server.Client()
	for range requestCount {
		wait.Go(func() {
			<-start
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, server.URL+"/v1/transfers", bytes.NewReader(body))
			if err != nil {
				results <- result{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", key)
			response, err := client.Do(request)
			if err != nil {
				results <- result{err: err}
				return
			}
			var responseTransfer transfer
			err = json.NewDecoder(response.Body).Decode(&responseTransfer)
			if closeErr := response.Body.Close(); err == nil {
				err = closeErr
			}
			results <- result{status: response.StatusCode, id: responseTransfer.ID, err: err}
		})
	}
	close(start)
	wait.Wait()
	close(results)

	transferIDs := map[string]struct{}{}
	for response := range results {
		if response.err != nil {
			t.Errorf("concurrent idempotent request failed: %v", response.err)
			continue
		}
		if response.status != http.StatusCreated {
			t.Errorf("expected replay status %d, got %d", http.StatusCreated, response.status)
		}
		transferIDs[response.id] = struct{}{}
	}
	if len(transferIDs) != 1 {
		t.Fatalf("expected every retry to return one transfer ID, got %d IDs", len(transferIDs))
	}
	var transferCount int
	if err := db.QueryRow(t.Context(), "SELECT count(*) FROM pgledger_transfers_view WHERE from_account_id = $1", fromAccount.ID).Scan(&transferCount); err != nil {
		t.Fatal(err)
	}
	if transferCount != 1 {
		t.Errorf("expected one ledger transfer after %d same-key requests, got %d", requestCount, transferCount)
	}
	var fromBalance, toBalance string
	if err := db.QueryRow(t.Context(), "SELECT balance::text FROM pgledger_accounts_view WHERE id = $1", fromAccount.ID).Scan(&fromBalance); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(t.Context(), "SELECT balance::text FROM pgledger_accounts_view WHERE id = $1", toAccount.ID).Scan(&toBalance); err != nil {
		t.Fatal(err)
	}
	if fromBalance != "-7.25" || toBalance != "7.25" {
		t.Errorf("same-key retries changed balances more than once: from=%s to=%s", fromBalance, toBalance)
	}
}

func TestReadUnknownAccountHTTP(t *testing.T) {
	db := testDatabase(t)
	handler := newHandler(db)
	accountID := "pgla_00000000000000000000000000"

	for _, path := range []string{"/v1/accounts/" + accountID, "/v1/accounts/" + accountID + "/entries"} {
		response := getHTTP(handler, path)
		if response.Code != http.StatusNotFound {
			t.Errorf("expected status %d for %s, got %d: %s", http.StatusNotFound, path, response.Code, response.Body.String())
		}
	}
}

func TestListAccountsHTTP(t *testing.T) {
	db := testDatabase(t)
	handler := newHandler(db)
	first := testhelpers.CreateAccount(t, db, "api-list-first", "USD")
	second := testhelpers.CreateAccount(t, db, "api-list-second", "USD")
	body, err := json.Marshal(transferRequest{FromAccountID: first.ID, ToAccountID: second.ID, Amount: "1.00"})
	if err != nil {
		t.Fatal(err)
	}
	transferResponse := transferRequestHTTP(t, handler, body, fmt.Sprintf("api-list-%d", time.Now().UnixNano()))
	if transferResponse.Code != http.StatusCreated {
		t.Fatalf("create transfer: expected status %d, got %d: %s", http.StatusCreated, transferResponse.Code, transferResponse.Body.String())
	}
	untouched := testhelpers.CreateAccount(t, db, "api-list-untouched", "USD")

	positions := map[string]int{}
	const pageSize = 200
	for offset := 0; ; offset += pageSize {
		response := getHTTP(handler, fmt.Sprintf("/v1/accounts?limit=%d&offset=%d", pageSize, offset))
		if response.Code != http.StatusOK {
			t.Fatalf("expected status %d, got %d: %s", http.StatusOK, response.Code, response.Body.String())
		}
		var accounts []account
		if err := json.NewDecoder(response.Body).Decode(&accounts); err != nil {
			t.Fatalf("decode account list: %v", err)
		}
		for index, account := range accounts {
			positions[account.ID] = offset + index
		}
		if len(accounts) < pageSize {
			break
		}
	}
	if _, found := positions[first.ID]; !found {
		t.Errorf("account list did not include transferred account %q", first.ID)
	}
	if _, found := positions[second.ID]; !found {
		t.Errorf("account list did not include transferred account %q", second.ID)
	}
	if positions[untouched.ID] >= positions[first.ID] || positions[untouched.ID] >= positions[second.ID] {
		t.Errorf("newly created untouched account should appear before accounts with older transfer activity; positions: first=%d second=%d untouched=%d", positions[first.ID], positions[second.ID], positions[untouched.ID])
	}
}

func TestOverdraftRejectionIsClearHTTP(t *testing.T) {
	db := testDatabase(t)
	handler := newHandler(db)
	request := httptest.NewRequest(http.MethodPost, "/v1/accounts", strings.NewReader(
		`{"name":"api-test-no-overdraft","currency":"USD","allow_negative_balance":false}`,
	))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusCreated {
		t.Fatalf("create source account: expected status %d, got %d: %s", http.StatusCreated, response.Code, response.Body.String())
	}
	var source account
	if err := json.NewDecoder(response.Body).Decode(&source); err != nil {
		t.Fatal(err)
	}
	target := testhelpers.CreateAccount(t, db, "api-test-overdraft-target", "USD")

	body, err := json.Marshal(transferRequest{FromAccountID: source.ID, ToAccountID: target.ID, Amount: "1.00"})
	if err != nil {
		t.Fatal(err)
	}
	transferResponse := transferRequestHTTP(t, handler, body, fmt.Sprintf("api-overdraft-%d", time.Now().UnixNano()))
	if transferResponse.Code != http.StatusUnprocessableEntity {
		t.Fatalf("expected status %d, got %d: %s", http.StatusUnprocessableEntity, transferResponse.Code, transferResponse.Body.String())
	}
	var responseBody map[string]string
	if err := json.NewDecoder(transferResponse.Body).Decode(&responseBody); err != nil {
		t.Fatal(err)
	}
	if responseBody["error"] != "Transfer would overdraw an account that disallows negative balances." {
		t.Errorf("unexpected overdraft error: %q", responseBody["error"])
	}
}

func TestConcurrentTransfersRespectBalanceConstraintHTTP(t *testing.T) {
	db := testDatabase(t)
	source := testhelpers.QueryOne[testhelpers.Account](t, db,
		"SELECT * FROM pgledger_create_account($1, $2, false)", "api-race-source", "USD")
	destination := testhelpers.CreateAccount(t, db, "api-race-destination", "USD")
	funding := testhelpers.CreateAccount(t, db, "api-race-funding", "USD")
	if _, err := testhelpers.CreateTransferReturnErr(t.Context(), db, funding.ID, source.ID, "100.00"); err != nil {
		t.Fatalf("fund race-test source account: %v", err)
	}

	httpServer := httptest.NewServer(newHandler(db))
	defer httpServer.Close()
	const requestCount = 10
	body, err := json.Marshal(transferRequest{
		FromAccountID: source.ID,
		ToAccountID:   destination.ID,
		Amount:        "20.00",
	})
	if err != nil {
		t.Fatal(err)
	}
	type requestResult struct {
		statusCode int
		body       string
		err        error
	}
	results := make(chan requestResult, requestCount)
	start := make(chan struct{})
	var wait sync.WaitGroup
	client := httpServer.Client()
	for index := range requestCount {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			<-start
			request, err := http.NewRequestWithContext(t.Context(), http.MethodPost, httpServer.URL+"/v1/transfers", bytes.NewReader(body))
			if err != nil {
				results <- requestResult{err: err}
				return
			}
			request.Header.Set("Content-Type", "application/json")
			request.Header.Set("Idempotency-Key", fmt.Sprintf("api-race-%d-%d", time.Now().UnixNano(), index))
			response, err := client.Do(request)
			if err != nil {
				results <- requestResult{err: err}
				return
			}
			responseBody, err := io.ReadAll(response.Body)
			if closeErr := response.Body.Close(); err == nil {
				err = closeErr
			}
			results <- requestResult{statusCode: response.StatusCode, body: string(responseBody), err: err}
		}(index)
	}
	close(start)
	wait.Wait()
	close(results)

	accepted, rejected := 0, 0
	for result := range results {
		if result.err != nil {
			t.Errorf("concurrent request failed: %v", result.err)
			continue
		}
		switch result.statusCode {
		case http.StatusCreated:
			accepted++
		case http.StatusUnprocessableEntity:
			rejected++
		default:
			t.Errorf("unexpected HTTP status %d: %s", result.statusCode, result.body)
		}
	}
	if accepted != 5 || rejected != 5 {
		t.Errorf("expected 5 accepted and 5 rejected transfers, got %d accepted and %d rejected", accepted, rejected)
	}

	var sourceAtZero, destinationAtHundred, fundingAtNegativeHundred bool
	var sourceVersion, destinationVersion int64
	if err := db.QueryRow(t.Context(), "SELECT balance = 0, version FROM pgledger_accounts_view WHERE id = $1", source.ID).Scan(&sourceAtZero, &sourceVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(t.Context(), "SELECT balance = 100, version FROM pgledger_accounts_view WHERE id = $1", destination.ID).Scan(&destinationAtHundred, &destinationVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(t.Context(), "SELECT balance = -100 FROM pgledger_accounts_view WHERE id = $1", funding.ID).Scan(&fundingAtNegativeHundred); err != nil {
		t.Fatal(err)
	}
	if !sourceAtZero || !destinationAtHundred || !fundingAtNegativeHundred {
		t.Errorf("unexpected balances after race: source_zero=%t destination_hundred=%t funding_negative_hundred=%t", sourceAtZero, destinationAtHundred, fundingAtNegativeHundred)
	}
	if sourceVersion != 6 || destinationVersion != 5 {
		t.Errorf("unexpected account versions after race: source=%d destination=%d", sourceVersion, destinationVersion)
	}
}

func TestInvalidHTTPRequests(t *testing.T) {
	handler := newHandler(nil)
	cases := []struct {
		name string
		path string
		body string
	}{
		{name: "malformed account JSON", path: "/v1/accounts", body: `{"name":`},
		{name: "missing transfer fields", path: "/v1/transfers", body: `{}`},
		{name: "unknown account field", path: "/v1/accounts", body: `{"name":"x","currency":"USD","unexpected":true}`},
		{name: "account name too long", path: "/v1/accounts", body: fmt.Sprintf(`{"name":%q,"currency":"USD"}`, strings.Repeat("x", maxAccountNameLength+1))},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, testCase.path, strings.NewReader(testCase.body))
			request.Header.Set("Content-Type", "application/json")
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != http.StatusBadRequest {
				t.Errorf("expected status %d, got %d: %s", http.StatusBadRequest, response.Code, response.Body.String())
			}
		})
	}
}

func TestOverlongIdempotencyKeyHTTP(t *testing.T) {
	handler := newHandler(nil)
	request := httptest.NewRequest(http.MethodPost, "/v1/transfers", strings.NewReader(
		`{"from_account_id":"from","to_account_id":"to","amount":"1.00"}`,
	))
	request.Header.Set("Idempotency-Key", strings.Repeat("k", maxIdempotencyLength+1))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, response.Code, response.Body.String())
	}
}

func TestInvalidPaginationHTTP(t *testing.T) {
	handler := newHandler(nil)
	response := getHTTP(handler, "/v1/accounts?limit=201")
	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s", http.StatusBadRequest, response.Code, response.Body.String())
	}
}

func TestDemoPageServed(t *testing.T) {
	handler := newHandler(nil)
	response := getHTTP(handler, "/")
	if response.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, response.Code)
	}
	if contentType := response.Header().Get("Content-Type"); !strings.HasPrefix(contentType, "text/html") {
		t.Errorf("expected HTML content type, got %q", contentType)
	}
	if !strings.Contains(response.Body.String(), "Ledger playground") || !strings.Contains(response.Body.String(), "Concurrency race test") {
		t.Error("response did not contain the expected demo content")
	}
}

func testDatabase(t *testing.T) *pgxpool.Pool {
	t.Helper()
	db, err := testhelpers.Connect(t.Context())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	if err := db.Ping(t.Context()); err != nil {
		db.Close()
		t.Fatalf("ping test database: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

func transferRequestHTTP(t *testing.T, handler http.Handler, body []byte, key string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, "/v1/transfers", bytes.NewReader(body))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", key)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func getHTTP(handler http.Handler, path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func decodeTransferResponse(t *testing.T, body io.Reader) transfer {
	t.Helper()
	var result transfer
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		t.Fatalf("decode transfer response: %v", err)
	}
	return result
}

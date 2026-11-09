package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

type loadAccount struct {
	ID      string `json:"id"`
	Balance string `json:"balance"`
	Version int64  `json:"version"`
}

type transferPayload struct {
	FromAccountID string `json:"from_account_id"`
	ToAccountID   string `json:"to_account_id"`
	Amount        string `json:"amount"`
}

type workerResult struct {
	latencies []time.Duration
	successes int
	failures  int
	statuses  map[int]int
	firstErr  string
}

func main() {
	apiURL := flag.String("url", "http://localhost:8080", "Base URL of the running pgledger API")
	duration := flag.Duration("duration", 10*time.Second, "How long to issue transfers")
	workers := flag.Int("workers", 20, "Number of concurrent HTTP workers")
	amount := flag.String("amount", "1.00", "Positive decimal amount for each transfer")
	username := flag.String("username", "", "HTTP Basic Auth username, when enabled")
	password := flag.String("password", "", "HTTP Basic Auth password, when enabled")
	flag.Parse()

	if *duration <= 0 || *workers <= 0 || strings.TrimSpace(*amount) == "" {
		fmt.Fprintln(os.Stderr, "duration and workers must be positive, and amount must not be empty")
		os.Exit(2)
		return
	}
	if (*username == "") != (*password == "") {
		fmt.Fprintln(os.Stderr, "username and password must be provided together")
		os.Exit(2)
		return
	}
	if err := runLoadCheck(strings.TrimRight(*apiURL, "/"), *duration, *workers, *amount, *username, *password); err != nil {
		fmt.Fprintf(os.Stderr, "API load check failed: %v\n", err)
		os.Exit(1)
	}
}

func runLoadCheck(apiURL string, duration time.Duration, workerCount int, amount, username, password string) error {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.MaxIdleConns = workerCount
	transport.MaxIdleConnsPerHost = workerCount
	transport.MaxConnsPerHost = workerCount
	var roundTripper http.RoundTripper = transport
	if username != "" {
		roundTripper = basicAuthTransport{base: transport, username: username, password: password}
	}
	client := &http.Client{Transport: roundTripper, Timeout: 30 * time.Second}
	defer transport.CloseIdleConnections()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, apiURL+"/healthz", nil)
	if err != nil {
		return err
	}
	response, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("connect to API at %s: %w", apiURL, err)
	}
	if err := response.Body.Close(); err != nil {
		return fmt.Errorf("close health-check response body: %w", err)
	}
	if response.StatusCode != http.StatusNoContent {
		return fmt.Errorf("health check returned HTTP %d; expected 204", response.StatusCode)
	}

	runID := fmt.Sprintf("%d", time.Now().UnixNano())
	source, err := createLoadAccount(client, apiURL, "api-load-source-"+runID)
	if err != nil {
		return fmt.Errorf("create source account: %w", err)
	}
	destination, err := createLoadAccount(client, apiURL, "api-load-destination-"+runID)
	if err != nil {
		return fmt.Errorf("create destination account: %w", err)
	}
	body, err := json.Marshal(transferPayload{FromAccountID: source.ID, ToAccountID: destination.ID, Amount: amount})
	if err != nil {
		return fmt.Errorf("encode transfer request: %w", err)
	}

	start := time.Now()
	deadline := start.Add(duration)
	results := make([]workerResult, workerCount)
	var wait sync.WaitGroup
	for workerID := range workerCount {
		wait.Add(1)
		go func(workerID int) {
			defer wait.Done()
			result := workerResult{statuses: make(map[int]int)}
			sequence := 0
			for time.Now().Before(deadline) {
				sequence++
				result.record(client, apiURL, body, runID, workerID, sequence)
			}
			results[workerID] = result
		}(workerID)
	}
	wait.Wait()
	elapsed := time.Since(start)

	allLatencies := make([]time.Duration, 0)
	failureCount, successCount := 0, 0
	statusCounts := make(map[int]int)
	firstError := ""
	for _, result := range results {
		allLatencies = append(allLatencies, result.latencies...)
		failureCount += result.failures
		successCount += result.successes
		for status, count := range result.statuses {
			statusCounts[status] += count
		}
		if firstError == "" && result.firstErr != "" {
			firstError = result.firstErr
		}
	}
	if len(allLatencies) == 0 {
		return errors.New("no transfer requests completed")
	}

	sourceAfter, err := getLoadAccount(client, apiURL, source.ID)
	if err != nil {
		return fmt.Errorf("read source account after load: %w", err)
	}
	destinationAfter, err := getLoadAccount(client, apiURL, destination.ID)
	if err != nil {
		return fmt.Errorf("read destination account after load: %w", err)
	}
	if sourceAfter.Version != int64(successCount) || destinationAfter.Version != int64(successCount) {
		return fmt.Errorf("account versions do not match successful requests: successful=%d source_version=%d destination_version=%d", successCount, sourceAfter.Version, destinationAfter.Version)
	}
	if !balancesNetToZero(sourceAfter.Balance, destinationAfter.Balance) {
		return fmt.Errorf("source and destination balances do not sum to zero: source=%s destination=%s", sourceAfter.Balance, destinationAfter.Balance)
	}

	slices.Sort(allLatencies)
	var latencySum time.Duration
	for _, latency := range allLatencies {
		latencySum += latency
	}
	avg := latencySum / time.Duration(len(allLatencies))
	p95 := percentile(allLatencies, 0.95)
	p99 := percentile(allLatencies, 0.99)
	requestsPerSecond := float64(successCount) / elapsed.Seconds()

	fmt.Printf("HTTP API load check\n")
	fmt.Printf("URL: %s\nDuration: %s (elapsed %s)\nWorkers: %d\nAmount per transfer: %s\n", apiURL, duration, elapsed.Round(time.Millisecond), workerCount, amount)
	fmt.Printf("Requests: %d total, %d successful, %d failed\n", successCount+failureCount, successCount, failureCount)
	fmt.Printf("Throughput: %.1f successful transfers/second\n", requestsPerSecond)
	fmt.Printf("Latency: average %s, p95 %s, p99 %s\n", avg.Round(time.Microsecond), p95.Round(time.Microsecond), p99.Round(time.Microsecond))
	fmt.Printf("HTTP status counts: %v\n", statusCounts)
	fmt.Printf("Source account version: %d; destination account version: %d\n", sourceAfter.Version, destinationAfter.Version)
	fmt.Printf("Balance check: %s + %s = 0\n", sourceAfter.Balance, destinationAfter.Balance)
	fmt.Printf("Load accounts: source=%s destination=%s\n", source.ID, destination.ID)
	if firstError != "" {
		fmt.Printf("First request error: %s\n", firstError)
	}
	if failureCount > 0 {
		return fmt.Errorf("%d transfer requests failed", failureCount)
	}
	return nil
}

type basicAuthTransport struct {
	base     http.RoundTripper
	username string
	password string
}

func (t basicAuthTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	requestWithAuth := request.Clone(request.Context())
	requestWithAuth.SetBasicAuth(t.username, t.password)
	return t.base.RoundTrip(requestWithAuth)
}

func (result *workerResult) record(client *http.Client, apiURL string, body []byte, runID string, workerID, sequence int) {
	request, err := http.NewRequest(http.MethodPost, apiURL+"/v1/transfers", bytes.NewReader(body))
	if err != nil {
		result.failures++
		result.firstErr = err.Error()
		return
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Idempotency-Key", fmt.Sprintf("api-load-%s-%d-%d", runID, workerID, sequence))
	started := time.Now()
	response, err := client.Do(request)
	if err != nil {
		result.latencies = append(result.latencies, time.Since(started))
		result.failures++
		if result.firstErr == "" {
			result.firstErr = err.Error()
		}
		return
	}
	_, readErr := io.Copy(io.Discard, response.Body)
	closeErr := response.Body.Close()
	result.latencies = append(result.latencies, time.Since(started))
	result.statuses[response.StatusCode]++
	if readErr != nil || closeErr != nil {
		result.failures++
		if result.firstErr == "" {
			if readErr != nil {
				result.firstErr = readErr.Error()
			} else {
				result.firstErr = closeErr.Error()
			}
		}
	} else if response.StatusCode != http.StatusCreated {
		result.failures++
		if result.firstErr == "" {
			result.firstErr = fmt.Sprintf("HTTP %d", response.StatusCode)
		}
	} else {
		result.successes++
	}
}

func createLoadAccount(client *http.Client, apiURL, name string) (loadAccount, error) {
	body, err := json.Marshal(map[string]any{
		"name":                   name,
		"currency":               "USD",
		"allow_negative_balance": true,
		"allow_positive_balance": true,
	})
	if err != nil {
		return loadAccount{}, err
	}
	request, err := http.NewRequest(http.MethodPost, apiURL+"/v1/accounts", bytes.NewReader(body))
	if err != nil {
		return loadAccount{}, err
	}
	request.Header.Set("Content-Type", "application/json")
	response, err := client.Do(request)
	if err != nil {
		return loadAccount{}, err
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return loadAccount{}, fmt.Errorf("read create-account response: %w", readErr)
	}
	if closeErr != nil {
		return loadAccount{}, fmt.Errorf("close create-account response: %w", closeErr)
	}
	if response.StatusCode != http.StatusCreated {
		return loadAccount{}, fmt.Errorf("HTTP %d: %s", response.StatusCode, responseBody)
	}
	var account loadAccount
	if err := json.Unmarshal(responseBody, &account); err != nil {
		return loadAccount{}, err
	}
	return account, nil
}

func getLoadAccount(client *http.Client, apiURL, accountID string) (loadAccount, error) {
	request, err := http.NewRequest(http.MethodGet, apiURL+"/v1/accounts/"+accountID, nil)
	if err != nil {
		return loadAccount{}, err
	}
	response, err := client.Do(request)
	if err != nil {
		return loadAccount{}, err
	}
	responseBody, readErr := io.ReadAll(response.Body)
	closeErr := response.Body.Close()
	if readErr != nil {
		return loadAccount{}, fmt.Errorf("read account response: %w", readErr)
	}
	if closeErr != nil {
		return loadAccount{}, fmt.Errorf("close account response: %w", closeErr)
	}
	if response.StatusCode != http.StatusOK {
		return loadAccount{}, fmt.Errorf("HTTP %d: %s", response.StatusCode, responseBody)
	}
	var account loadAccount
	if err := json.Unmarshal(responseBody, &account); err != nil {
		return loadAccount{}, err
	}
	return account, nil
}

func percentile(sortedValues []time.Duration, percentile float64) time.Duration {
	index := int(float64(len(sortedValues)-1) * percentile)
	return sortedValues[index]
}

func balancesNetToZero(first, second string) bool {
	firstValue, ok := new(big.Rat).SetString(first)
	if !ok {
		return false
	}
	secondValue, ok := new(big.Rat).SetString(second)
	if !ok {
		return false
	}
	return new(big.Rat).Add(firstValue, secondValue).Sign() == 0
}

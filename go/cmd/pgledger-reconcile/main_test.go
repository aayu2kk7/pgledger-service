package main

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"

	"github.com/pgr0ss/pgledger/testhelpers"
)

func TestScanReportsHealthyLedger(t *testing.T) {
	db, err := testhelpers.Connect(t.Context())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(db.Close)

	result, err := scan(t.Context(), db)
	if err != nil {
		t.Fatalf("scan ledger: %v", err)
	}
	if !result.Healthy {
		t.Fatalf("expected a healthy ledger, got %+v", result)
	}
	if len(result.Checks) != len(auditChecks) {
		t.Fatalf("expected %d checks, got %d", len(auditChecks), len(result.Checks))
	}
	for _, check := range result.Checks {
		if check.ViolationCount != 0 {
			t.Errorf("check %q found %d violations: %v", check.Name, check.ViolationCount, check.Examples)
		}
	}
}

func TestCollectReportDetectsAccountBalanceTampering(t *testing.T) {
	db, err := testhelpers.Connect(t.Context())
	if err != nil {
		t.Fatalf("connect to test database: %v", err)
	}
	t.Cleanup(db.Close)

	tx, err := db.Begin(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	var accountID string
	if err := tx.QueryRow(t.Context(), "SELECT id FROM pgledger_create_account($1, $2)", "reconcile-test-tamper", "USD").Scan(&accountID); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(t.Context(), "UPDATE pgledger_accounts SET balance = 1 WHERE id = $1", accountID); err != nil {
		t.Fatal(err)
	}

	result, err := collectReport(t.Context(), tx)
	if err != nil {
		t.Fatalf("inspect tampered ledger: %v", err)
	}
	if result.Healthy {
		t.Fatal("expected a balance tamper to make the ledger unhealthy")
	}
	checks := make(map[string]checkResult, len(result.Checks))
	for _, check := range result.Checks {
		checks[check.Name] = check
	}
	if checks["account_balances_and_versions"].ViolationCount != 1 {
		t.Errorf("expected the account balance check to identify one mismatch, got %+v", checks["account_balances_and_versions"])
	}
	if checks["global_debit_credit_conservation"].ViolationCount != 0 {
		t.Errorf("a balance-only edit should preserve the entry sum, got %+v", checks["global_debit_credit_conservation"])
	}
}

func TestWriteReportProducesJSON(t *testing.T) {
	input := report{Healthy: false, Checks: []checkResult{{Name: "test", ViolationCount: 1, Examples: []string{"pglt_example"}}}}
	var output bytes.Buffer
	if err := writeReport(&output, input); err != nil {
		t.Fatalf("write report: %v", err)
	}
	var decoded report
	if err := json.Unmarshal(output.Bytes(), &decoded); err != nil {
		t.Fatalf("decode report JSON: %v", err)
	}
	if decoded.Healthy || len(decoded.Checks) != 1 || decoded.Checks[0].ViolationCount != 1 {
		t.Fatalf("unexpected report JSON: %+v", decoded)
	}
}

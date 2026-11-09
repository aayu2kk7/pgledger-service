package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgr0ss/pgledger/internal/dburl"
)

const maxExamples = 5

type check struct {
	name  string
	query string
}

type checkResult struct {
	Name           string   `json:"name"`
	ViolationCount int64    `json:"violation_count"`
	Examples       []string `json:"example_ids,omitempty"`
}

type report struct {
	CheckedAt time.Time     `json:"checked_at"`
	Healthy   bool          `json:"healthy"`
	Checks    []checkResult `json:"checks"`
}

var auditChecks = []check{
	{
		name: "transfer_amounts_and_accounts",
		query: `
			SELECT t.id AS violation_id
			FROM pgledger_transfers t
			WHERE t.amount <= 0
				OR t.amount >= 'Infinity'::numeric
				OR t.from_account_id = t.to_account_id`,
	},
	{
		name: "transfer_entry_pairs",
		query: `
			SELECT t.id AS violation_id
			FROM pgledger_transfers t
			LEFT JOIN pgledger_entries e ON e.transfer_id = t.id
			GROUP BY t.id, t.from_account_id, t.to_account_id, t.amount
			HAVING count(e.id) <> 2
				OR count(e.id) FILTER (
					WHERE e.account_id = t.from_account_id AND e.amount = -t.amount
				) <> 1
				OR count(e.id) FILTER (
					WHERE e.account_id = t.to_account_id AND e.amount = t.amount
				) <> 1`,
	},
	{
		name: "orphan_entries",
		query: `
			SELECT e.id AS violation_id
			FROM pgledger_entries e
			LEFT JOIN pgledger_transfers t ON t.id = e.transfer_id
			WHERE t.id IS NULL`,
	},
	{
		name: "account_balances_and_versions",
		query: `
			SELECT a.id AS violation_id
			FROM pgledger_accounts a
			LEFT JOIN pgledger_entries e ON e.account_id = a.id
			GROUP BY a.id, a.balance, a.version
			HAVING a.balance <> COALESCE(sum(e.amount), 0)
				OR a.version <> count(e.id)`,
	},
	{
		name: "account_balance_policies",
		query: `
			SELECT id AS violation_id
			FROM pgledger_accounts
			WHERE (NOT allow_negative_balance AND balance < 0)
				OR (NOT allow_positive_balance AND balance > 0)`,
	},
	{
		name: "account_entry_chains",
		query: `
			WITH ordered_entries AS (
				SELECT account_id,
					account_version,
					account_previous_balance,
					account_current_balance,
					amount,
					row_number() OVER (
						PARTITION BY account_id ORDER BY account_version
					) AS expected_version,
					lag(account_current_balance) OVER (
						PARTITION BY account_id ORDER BY account_version
					) AS prior_balance
				FROM pgledger_entries
			)
			SELECT DISTINCT account_id AS violation_id
			FROM ordered_entries
			WHERE account_version <> expected_version
				OR account_previous_balance <> COALESCE(prior_balance, 0)
				OR account_current_balance <> account_previous_balance + amount`,
	},
	{
		name: "global_debit_credit_conservation",
		query: `
			SELECT 'ledger'::text AS violation_id
			WHERE COALESCE((SELECT sum(amount) FROM pgledger_entries), 0) <> 0`,
	},
	{
		name: "transfer_account_currencies",
		query: `
			SELECT t.id AS violation_id
			FROM pgledger_transfers t
			JOIN pgledger_accounts source ON source.id = t.from_account_id
			JOIN pgledger_accounts destination ON destination.id = t.to_account_id
			WHERE source.currency <> destination.currency`,
	},
}

func main() {
	os.Exit(run())
}

func run() int {
	interval := flag.Duration("interval", 10*time.Second, "time between full reconciliation scans")
	once := flag.Bool("once", false, "run one scan and exit with a nonzero status if violations are found")
	flag.Parse()
	if *interval <= 0 {
		fmt.Fprintln(os.Stderr, "interval must be positive")
		return 2
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = dburl.URL()
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Printf("create PostgreSQL pool: %v", err)
		return 1
	}
	defer pool.Close()
	if err := pool.Ping(ctx); err != nil {
		log.Printf("ping PostgreSQL: %v", err)
		return 1
	}

	if *once {
		result, err := scan(ctx, pool)
		if err != nil {
			log.Printf("reconciliation scan failed: %v", err)
			return 1
		}
		if err := writeReport(os.Stdout, result); err != nil {
			log.Printf("write reconciliation report: %v", err)
			return 1
		}
		if !result.Healthy {
			return 1
		}
		return 0
	}

	if err := scanAndReport(ctx, pool); err != nil {
		log.Printf("initial reconciliation scan failed: %v", err)
	}
	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return 0
		case <-ticker.C:
			if err := scanAndReport(ctx, pool); err != nil {
				log.Printf("reconciliation scan failed: %v", err)
			}
		}
	}
}

func scanAndReport(ctx context.Context, pool *pgxpool.Pool) error {
	result, err := scan(ctx, pool)
	if err != nil {
		return err
	}
	if err := writeReport(os.Stdout, result); err != nil {
		return err
	}
	if !result.Healthy {
		log.Printf("CRITICAL: ledger reconciliation found invariant violations")
	}
	return nil
}

func scan(ctx context.Context, pool *pgxpool.Pool) (report, error) {
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.RepeatableRead,
		AccessMode: pgx.ReadOnly,
	})
	if err != nil {
		return report{}, fmt.Errorf("begin read-only snapshot: %w", err)
	}
	defer func() { _ = tx.Rollback(context.Background()) }()
	result, err := collectReport(ctx, tx)
	if err != nil {
		return report{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return report{}, fmt.Errorf("finish read-only snapshot: %w", err)
	}
	return result, nil
}

func collectReport(ctx context.Context, tx pgx.Tx) (report, error) {
	result := report{CheckedAt: time.Now().UTC(), Healthy: true, Checks: make([]checkResult, 0, len(auditChecks))}
	for _, invariant := range auditChecks {
		checkResult, err := runCheck(ctx, tx, invariant)
		if err != nil {
			return report{}, fmt.Errorf("check %s: %w", invariant.name, err)
		}
		if checkResult.ViolationCount > 0 {
			result.Healthy = false
		}
		result.Checks = append(result.Checks, checkResult)
	}
	return result, nil
}

func runCheck(ctx context.Context, tx pgx.Tx, invariant check) (checkResult, error) {
	query := fmt.Sprintf(`
		SELECT violation_id, count(*) OVER ()
		FROM (%s) AS violations
		ORDER BY violation_id
		LIMIT $1`, invariant.query)
	rows, err := tx.Query(ctx, query, maxExamples)
	if err != nil {
		return checkResult{}, err
	}
	defer rows.Close()

	result := checkResult{Name: invariant.name, Examples: []string{}}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id, &result.ViolationCount); err != nil {
			return checkResult{}, err
		}
		result.Examples = append(result.Examples, id)
	}
	if err := rows.Err(); err != nil {
		return checkResult{}, err
	}
	return result, nil
}

func writeReport(output io.Writer, result report) error {
	encoder := json.NewEncoder(output)
	encoder.SetEscapeHTML(false)
	return encoder.Encode(result)
}

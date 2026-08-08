package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/MagicRodri/customer-service/internal/domain"
	"github.com/MagicRodri/customer-service/migrations"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store { return &Store{pool: pool} }

// Tx is the unit of work handed to callers. Every method on it runs inside the
// same database transaction, which is what makes the outbox write atomic with
// the state change it describes.
type Tx struct {
	tx pgx.Tx
}

// InTx runs fn inside a transaction, committing when fn returns nil.
func (s *Store) InTx(ctx context.Context, fn func(context.Context, *Tx) error) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := fn(ctx, &Tx{tx: tx}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) Migrate(ctx context.Context) error {
	_, err := s.pool.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (
		version TEXT PRIMARY KEY, applied_at TIMESTAMPTZ NOT NULL DEFAULT now())`)
	if err != nil {
		return fmt.Errorf("create schema_migrations: %w", err)
	}

	entries, err := migrations.FS.ReadDir(".")
	if err != nil {
		return fmt.Errorf("read migrations: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)

	for _, name := range names {
		var exists bool
		if err := s.pool.QueryRow(ctx,
			`SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, name,
		).Scan(&exists); err != nil {
			return fmt.Errorf("check migration %s: %w", name, err)
		}
		if exists {
			continue
		}
		body, err := migrations.FS.ReadFile(name)
		if err != nil {
			return fmt.Errorf("read migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("apply migration %s: %w", name, err)
		}
		if _, err := s.pool.Exec(ctx,
			`INSERT INTO schema_migrations (version) VALUES ($1)`, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
	}
	return nil
}

const customerColumns = `id, email, name, tier, status, lifetime_spend_cents, created_at, updated_at`

func scanCustomer(row pgx.Row) (domain.Customer, error) {
	var c domain.Customer
	err := row.Scan(&c.ID, &c.Email, &c.Name, &c.Tier, &c.Status,
		&c.LifetimeSpendCents, &c.CreatedAt, &c.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return c, domain.ErrNotFound
	}
	return c, err
}

func (s *Store) GetCustomer(ctx context.Context, id string) (domain.Customer, error) {
	return scanCustomer(s.pool.QueryRow(ctx,
		`SELECT `+customerColumns+` FROM customers WHERE id = $1`, id))
}

func (s *Store) ListCustomers(ctx context.Context, limit int) ([]domain.Customer, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT `+customerColumns+` FROM customers ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []domain.Customer{}
	for rows.Next() {
		c, err := scanCustomer(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// GetCustomerForUpdate locks the row for the rest of the transaction so that a
// concurrent order event and an API call cannot interleave a lost update.
func (t *Tx) GetCustomerForUpdate(ctx context.Context, id string) (domain.Customer, error) {
	return scanCustomer(t.tx.QueryRow(ctx,
		`SELECT `+customerColumns+` FROM customers WHERE id = $1 FOR UPDATE`, id))
}

func (t *Tx) InsertCustomer(ctx context.Context, c domain.Customer) error {
	_, err := t.tx.Exec(ctx,
		`INSERT INTO customers (id, email, name, tier, status, lifetime_spend_cents, created_at, updated_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		c.ID, c.Email, c.Name, c.Tier, c.Status, c.LifetimeSpendCents, c.CreatedAt, c.UpdatedAt)

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) && pgErr.Code == "23505" {
		return domain.ErrEmailTaken
	}
	return err
}

func (t *Tx) UpdateCustomer(ctx context.Context, c domain.Customer) error {
	_, err := t.tx.Exec(ctx,
		`UPDATE customers SET name = $2, tier = $3, status = $4,
		        lifetime_spend_cents = $5, updated_at = $6
		 WHERE id = $1`,
		c.ID, c.Name, c.Tier, c.Status, c.LifetimeSpendCents, c.UpdatedAt)
	return err
}

// OutboxRecord is one business event, staged for the outbox connector.
type OutboxRecord struct {
	AggregateType string
	AggregateID   string
	EventType     string
	Payload       any
	TraceID       string

	// Channel selects the destination topic: the router rewrites it to
	// business.<channel>.events. Leave it empty to fall back to one topic per
	// aggregate type.
	Channel string
}

func (t *Tx) AppendOutbox(ctx context.Context, rec OutboxRecord) error {
	payload, err := json.Marshal(rec.Payload)
	if err != nil {
		return fmt.Errorf("marshal outbox payload: %w", err)
	}
	channel := rec.Channel
	if channel == "" {
		channel = rec.AggregateType
	}
	_, err = t.tx.Exec(ctx,
		`INSERT INTO outbox (id, aggregate_type, aggregate_id, event_type, channel, payload, trace_id, created_at)
		 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		uuid.NewString(), rec.AggregateType, rec.AggregateID, rec.EventType,
		channel, payload, rec.TraceID, time.Now().UTC())
	return err
}

// MarkProcessed records an inbound event ID. It reports false when the event
// was already applied, which is the consumer's deduplication check.
func (t *Tx) MarkProcessed(ctx context.Context, eventID, sourceTopic string) (bool, error) {
	tag, err := t.tx.Exec(ctx,
		`INSERT INTO processed_events (event_id, source_topic) VALUES ($1, $2)
		 ON CONFLICT (event_id) DO NOTHING`, eventID, sourceTopic)
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

func (s *Store) AppendTechnicalAudit(ctx context.Context, source, operation, rowKey string, before, after []byte) error {
	_, err := s.pool.Exec(ctx,
		`INSERT INTO technical_audit_log (source, operation, row_key, before_row, after_row)
		 VALUES ($1, $2, $3, $4, $5)`, source, operation, rowKey, before, after)
	return err
}

func (s *Store) Ping(ctx context.Context) error { return s.pool.Ping(ctx) }

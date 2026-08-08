package app

import (
	"context"
	"fmt"
	"log/slog"
	"net/mail"
	"strings"
	"time"

	"github.com/MagicRodri/customer-service/internal/domain"
	"github.com/MagicRodri/customer-service/internal/store"
	"github.com/google/uuid"
)

type App struct {
	store *store.Store
	log   *slog.Logger
}

func New(s *store.Store, log *slog.Logger) *App {
	return &App{store: s, log: log}
}

func (a *App) GetCustomer(ctx context.Context, id string) (domain.Customer, error) {
	return a.store.GetCustomer(ctx, id)
}

func (a *App) ListCustomers(ctx context.Context, limit int) ([]domain.Customer, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	return a.store.ListCustomers(ctx, limit)
}

// CreateCustomer writes the customer row and its CustomerCreated outbox row in
// one transaction. Either both land or neither does, so the event stream can
// never disagree with the table it describes.
func (a *App) CreateCustomer(ctx context.Context, email, name string) (domain.Customer, error) {
	email = strings.TrimSpace(strings.ToLower(email))
	name = strings.TrimSpace(name)
	if _, err := mail.ParseAddress(email); err != nil {
		return domain.Customer{}, fmt.Errorf("%w: email is not a valid address", domain.ErrInvalidInput)
	}
	if name == "" {
		return domain.Customer{}, fmt.Errorf("%w: name is required", domain.ErrInvalidInput)
	}

	now := time.Now().UTC()
	customer := domain.Customer{
		ID:        uuid.NewString(),
		Email:     email,
		Name:      name,
		Tier:      domain.TierStandard,
		Status:    domain.StatusActive,
		CreatedAt: now,
		UpdatedAt: now,
	}

	err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		if err := tx.InsertCustomer(ctx, customer); err != nil {
			return err
		}
		return tx.AppendOutbox(ctx, store.OutboxRecord{
			AggregateType: aggregateTypeCustomer,
			AggregateID:   customer.ID,
			EventType:     "CustomerCreated",
			Payload:       newCustomerCreated(customer),
		})
	})
	if err != nil {
		return domain.Customer{}, err
	}
	return customer, nil
}

func (a *App) BlockCustomer(ctx context.Context, id, reason string) (domain.Customer, error) {
	return a.setStatus(ctx, id, domain.StatusBlocked, reason)
}

func (a *App) UnblockCustomer(ctx context.Context, id string) (domain.Customer, error) {
	return a.setStatus(ctx, id, domain.StatusActive, "")
}

func (a *App) setStatus(ctx context.Context, id string, status domain.Status, reason string) (domain.Customer, error) {
	var updated domain.Customer

	err := a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		customer, err := tx.GetCustomerForUpdate(ctx, id)
		if err != nil {
			return err
		}
		if customer.Status == status {
			return domain.ErrAlreadyInThat
		}

		customer.Status = status
		customer.UpdatedAt = time.Now().UTC()
		if err := tx.UpdateCustomer(ctx, customer); err != nil {
			return err
		}
		updated = customer

		var payload any
		eventType := "CustomerUnblocked"
		if status == domain.StatusBlocked {
			eventType = "CustomerBlocked"
			payload = customerBlocked{
				EventID:    uuid.NewString(),
				EventType:  eventType,
				OccurredAt: nowRFC3339(),
				CustomerID: customer.ID,
				Reason:     reason,
			}
		} else {
			payload = customerUnblocked{
				EventID:    uuid.NewString(),
				EventType:  eventType,
				OccurredAt: nowRFC3339(),
				CustomerID: customer.ID,
			}
		}

		return tx.AppendOutbox(ctx, store.OutboxRecord{
			AggregateType: aggregateTypeCustomer,
			AggregateID:   customer.ID,
			EventType:     eventType,
			Payload:       payload,
		})
	})
	if err != nil {
		return domain.Customer{}, err
	}
	return updated, nil
}

// applyOrderSpend is the reactive half of the loop: an order confirmed by the
// order service raises the customer's lifetime spend, and crossing a threshold
// emits CustomerTierChanged, which the order service then uses to price the
// next order. Deduplication, the spend update and the new outbox row all share
// one transaction.
func (a *App) applyOrderSpend(ctx context.Context, eventID, topic, customerID string, amountCents int64) error {
	return a.store.InTx(ctx, func(ctx context.Context, tx *store.Tx) error {
		fresh, err := tx.MarkProcessed(ctx, eventID, topic)
		if err != nil {
			return err
		}
		if !fresh {
			a.log.Debug("skipping already-processed event", "event_id", eventID)
			return nil
		}

		customer, err := tx.GetCustomerForUpdate(ctx, customerID)
		if err != nil {
			return err
		}

		previousTier := customer.Tier
		customer.LifetimeSpendCents = max(0, customer.LifetimeSpendCents+amountCents)
		customer.Tier = domain.TierFor(customer.LifetimeSpendCents)
		customer.UpdatedAt = time.Now().UTC()
		if err := tx.UpdateCustomer(ctx, customer); err != nil {
			return err
		}
		if customer.Tier == previousTier {
			return nil
		}

		a.log.Info("customer tier changed",
			"customer_id", customer.ID, "from", previousTier, "to", customer.Tier)

		return tx.AppendOutbox(ctx, store.OutboxRecord{
			AggregateType: aggregateTypeCustomer,
			AggregateID:   customer.ID,
			EventType:     "CustomerTierChanged",
			Payload: customerTierChanged{
				EventID:            uuid.NewString(),
				EventType:          "CustomerTierChanged",
				OccurredAt:         nowRFC3339(),
				CustomerID:         customer.ID,
				PreviousTier:       string(previousTier),
				Tier:               string(customer.Tier),
				DiscountBps:        customer.Tier.DiscountBps(),
				LifetimeSpendCents: customer.LifetimeSpendCents,
			},
		})
	})
}

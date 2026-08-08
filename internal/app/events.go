package app

import (
	"time"

	"github.com/MagicRodri/customer-service/internal/domain"
	"github.com/google/uuid"
)

// Outbox payloads. Field names and types mirror schemas/*.avsc; the outbox
// connector expands this JSON into the Avro record published on
// business.customer.events.
const aggregateTypeCustomer = "customer"

type customerCreated struct {
	EventID     string `json:"event_id"`
	EventType   string `json:"event_type"`
	OccurredAt  string `json:"occurred_at"`
	CustomerID  string `json:"customer_id"`
	Email       string `json:"email"`
	Name        string `json:"name"`
	Tier        string `json:"tier"`
	Status      string `json:"status"`
	DiscountBps int32  `json:"discount_bps"`
}

type customerBlocked struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	CustomerID string `json:"customer_id"`
	Reason     string `json:"reason"`
}

type customerUnblocked struct {
	EventID    string `json:"event_id"`
	EventType  string `json:"event_type"`
	OccurredAt string `json:"occurred_at"`
	CustomerID string `json:"customer_id"`
}

type customerTierChanged struct {
	EventID            string `json:"event_id"`
	EventType          string `json:"event_type"`
	OccurredAt         string `json:"occurred_at"`
	CustomerID         string `json:"customer_id"`
	PreviousTier       string `json:"previous_tier"`
	Tier               string `json:"tier"`
	DiscountBps        int32  `json:"discount_bps"`
	LifetimeSpendCents int64  `json:"lifetime_spend_cents"`
}

func newCustomerCreated(c domain.Customer) customerCreated {
	return customerCreated{
		EventID:     uuid.NewString(),
		EventType:   "CustomerCreated",
		OccurredAt:  nowRFC3339(),
		CustomerID:  c.ID,
		Email:       c.Email,
		Name:        c.Name,
		Tier:        string(c.Tier),
		Status:      string(c.Status),
		DiscountBps: c.Tier.DiscountBps(),
	}
}

func nowRFC3339() string { return time.Now().UTC().Format(time.RFC3339Nano) }

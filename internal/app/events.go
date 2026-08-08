package app

import (
	"time"

	"github.com/MagicRodri/customer-service/internal/domain"
	"github.com/google/uuid"
)

// Outbox payloads. Field names and types mirror schemas/*.avsc; the outbox
// connector expands this JSON into the Avro record published on the channel's
// topic.
const aggregateTypeCustomer = "customer"

// Channels split this domain's events across topics. The connector routes
// purely on the channel column and knows nothing about event types, so carving
// out a new topic is a change to this table alone — no connector edit, and no
// interruption for consumers that subscribe by pattern.
//
// A consumer matching ^business\.customer\..* keeps receiving everything; one
// that only cares about loyalty can subscribe to that single topic instead.
const (
	channelCustomerLifecycle = "customer.lifecycle" // → business.customer.lifecycle.events
	channelCustomerLoyalty   = "customer.loyalty"   // → business.customer.loyalty.events
)

// channelFor maps an event type to its topic. An unrecognised type falls back
// to the domain-wide channel rather than landing somewhere unroutable.
func channelFor(eventType string) string {
	switch eventType {
	case "CustomerCreated", "CustomerBlocked", "CustomerUnblocked":
		return channelCustomerLifecycle
	case "CustomerTierChanged":
		return channelCustomerLoyalty
	default:
		return aggregateTypeCustomer
	}
}

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

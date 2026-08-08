package app

import (
	"context"
	"errors"
	"fmt"

	"github.com/MagicRodri/customer-service/internal/domain"
	"github.com/MagicRodri/customer-service/internal/eventing"
)

// HandleBusinessEvent reacts to the order domain's outbox events. These are
// versioned contracts owned by the order service; nothing here depends on that
// service's table layout.
func (a *App) HandleBusinessEvent(ctx context.Context, msg eventing.Message) error {
	if msg.Value == nil {
		return nil // tombstone from outbox retention cleanup
	}

	eventType := msg.EventType()
	amount := eventing.Int64(msg.Value, "total_cents")
	switch eventType {
	case "OrderCreated":
	case "OrderCancelled":
		// A cancellation reverses the spend it once contributed, which can also
		// demote the customer back down a tier.
		amount = -amount
	default:
		a.log.Debug("ignoring business event", "type", eventType, "topic", msg.Topic)
		return nil
	}

	eventID := eventing.String(msg.Value, "event_id")
	customerID := eventing.String(msg.Value, "customer_id")
	if eventID == "" || customerID == "" {
		return fmt.Errorf("malformed %s: event_id=%q customer_id=%q; record actually carries %v",
			eventType, eventID, customerID, eventing.FieldNames(msg.Value))
	}

	err := a.applyOrderSpend(ctx, eventID, msg.Topic, customerID, amount)
	if errors.Is(err, domain.ErrNotFound) {
		// The order service accepted an order for a customer this service has
		// no row for. Retrying cannot fix that, so it is logged and dropped.
		a.log.Warn("order event for unknown customer",
			"type", eventType, "customer_id", customerID, "event_id", eventID)
		return nil
	}
	return err
}

// HandleTechnicalEvent stores raw CDC rows for this service's own tables.
//
// The technical stream mirrors the physical schema and changes whenever a
// column does, so it is deliberately confined to audit. Cross-service reactions
// belong on the business stream above.
func (a *App) HandleTechnicalEvent(ctx context.Context, msg eventing.Message) error {
	if msg.Value == nil {
		return nil // Debezium tombstone following a delete
	}

	operation := eventing.String(msg.Value, "op")
	before, _ := eventing.Record(msg.Value, "before")
	after, _ := eventing.Record(msg.Value, "after")

	beforeJSON, err := eventing.JSON(before)
	if err != nil {
		return fmt.Errorf("encode before image: %w", err)
	}
	afterJSON, err := eventing.JSON(after)
	if err != nil {
		return fmt.Errorf("encode after image: %w", err)
	}

	rowKey := eventing.String(after, "id")
	if rowKey == "" {
		rowKey = eventing.String(before, "id")
	}

	return a.store.AppendTechnicalAudit(ctx, msg.Topic, operation, rowKey, beforeJSON, afterJSON)
}

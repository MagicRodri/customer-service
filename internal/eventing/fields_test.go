package eventing

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/hamba/avro/v2"
)

// Every published contract must parse as Avro, or the connector would reject it
// at registration time rather than here.
func TestPublishedSchemasParse(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join("..", "..", "schemas", "*.avsc"))
	if err != nil || len(paths) == 0 {
		t.Fatalf("no schemas found: %v", err)
	}
	for _, path := range paths {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if _, err := avro.Parse(string(body)); err != nil {
			t.Errorf("%s does not parse: %v", filepath.Base(path), err)
		}
	}
}

// Decoding a real Avro payload exercises the same path the consumer uses, so
// the accessors are checked against hamba's actual output types rather than
// hand-built maps.
func TestAccessorsOnDecodedRecord(t *testing.T) {
	body, err := os.ReadFile(filepath.Join("..", "..", "schemas", "CustomerTierChanged.avsc"))
	if err != nil {
		t.Fatal(err)
	}
	schema, err := avro.Parse(string(body))
	if err != nil {
		t.Fatal(err)
	}

	encoded, err := avro.Marshal(schema, map[string]any{
		"event_id":             "8f1d9e0a-0000-4000-8000-000000000001",
		"event_type":           "CustomerTierChanged",
		"occurred_at":          "2026-01-01T00:00:00Z",
		"customer_id":          "c-1",
		"previous_tier":        "STANDARD",
		"tier":                 "GOLD",
		"discount_bps":         500,
		"lifetime_spend_cents": int64(75_000),
	})
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	decoded := map[string]any{}
	if err := avro.Unmarshal(schema, encoded, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if got := String(decoded, "tier"); got != "GOLD" {
		t.Errorf("tier = %q, want GOLD", got)
	}
	if got := Int64(decoded, "lifetime_spend_cents"); got != 75_000 {
		t.Errorf("lifetime_spend_cents = %d, want 75000", got)
	}
	if got := Int64(decoded, "discount_bps"); got != 500 {
		t.Errorf("discount_bps = %d, want 500", got)
	}
	if got := String(decoded, "absent_field"); got != "" {
		t.Errorf("absent field = %q, want empty", got)
	}
}

// Debezium models optional columns as Avro unions, which decode to a
// single-key map. The accessors must see through that wrapper.
func TestAccessorsUnwrapUnions(t *testing.T) {
	record := map[string]any{
		"reason": map[string]any{"string": "fraud"},
		"amount": map[string]any{"long": int64(42)},
		"absent": nil,
	}
	if got := String(record, "reason"); got != "fraud" {
		t.Errorf("reason = %q, want fraud", got)
	}
	if got := Int64(record, "amount"); got != 42 {
		t.Errorf("amount = %d, want 42", got)
	}
	if got := String(record, "absent"); got != "" {
		t.Errorf("absent = %q, want empty", got)
	}
}

func TestEventTypePrefersHeader(t *testing.T) {
	msg := Message{
		Headers: map[string]string{"eventType": "CustomerBlocked"},
		Value:   map[string]any{"event_type": "CustomerCreated"},
	}
	if got := msg.EventType(); got != "CustomerBlocked" {
		t.Errorf("EventType() = %q, want CustomerBlocked", got)
	}

	msg.Headers = nil
	if got := msg.EventType(); got != "CustomerCreated" {
		t.Errorf("EventType() fallback = %q, want CustomerCreated", got)
	}
}

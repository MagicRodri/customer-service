package domain

import (
	"errors"
	"time"
)

type Tier string

const (
	TierStandard Tier = "STANDARD"
	TierGold     Tier = "GOLD"
	TierPlatinum Tier = "PLATINUM"
)

type Status string

const (
	StatusActive  Status = "ACTIVE"
	StatusBlocked Status = "BLOCKED"
)

var (
	ErrNotFound      = errors.New("customer not found")
	ErrEmailTaken    = errors.New("email already registered")
	ErrInvalidInput  = errors.New("invalid input")
	ErrAlreadyInThat = errors.New("customer already in that state")
)

type Customer struct {
	ID                string    `json:"id"`
	Email             string    `json:"email"`
	Name              string    `json:"name"`
	Tier              Tier      `json:"tier"`
	Status            Status    `json:"status"`
	LifetimeSpendCents int64    `json:"lifetime_spend_cents"`
	CreatedAt         time.Time `json:"created_at"`
	UpdatedAt         time.Time `json:"updated_at"`
}

// DiscountBps is the tier's discount in basis points. Order service receives
// this value through CustomerTierChanged and applies it when pricing an order.
func (t Tier) DiscountBps() int32 {
	switch t {
	case TierGold:
		return 500
	case TierPlatinum:
		return 1000
	default:
		return 0
	}
}

// TierFor maps lifetime spend to the tier it earns. Thresholds are deliberately
// low so the event loop is observable in a demo run.
func TierFor(lifetimeSpendCents int64) Tier {
	switch {
	case lifetimeSpendCents >= 200_000:
		return TierPlatinum
	case lifetimeSpendCents >= 50_000:
		return TierGold
	default:
		return TierStandard
	}
}

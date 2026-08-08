package app

import "testing"

func TestChannelForRoutesEveryPublishedEvent(t *testing.T) {
	cases := map[string]string{
		"CustomerCreated":     channelCustomerLifecycle,
		"CustomerBlocked":     channelCustomerLifecycle,
		"CustomerUnblocked":   channelCustomerLifecycle,
		"CustomerTierChanged": channelCustomerLoyalty,
	}
	for eventType, want := range cases {
		if got := channelFor(eventType); got != want {
			t.Errorf("channelFor(%q) = %q, want %q", eventType, got, want)
		}
	}
}

// An unmapped type must still route somewhere valid: an empty channel would
// produce the topic `business..events`.
func TestChannelForFallsBackToAggregateType(t *testing.T) {
	if got := channelFor("SomethingAddedLater"); got != aggregateTypeCustomer {
		t.Errorf("channelFor(unknown) = %q, want %q", got, aggregateTypeCustomer)
	}
	if channelFor("SomethingAddedLater") == "" {
		t.Error("fallback channel must never be empty")
	}
}

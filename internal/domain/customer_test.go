package domain

import "testing"

func TestTierForThresholds(t *testing.T) {
	cases := []struct {
		spend int64
		want  Tier
	}{
		{0, TierStandard},
		{49_999, TierStandard},
		{50_000, TierGold},
		{199_999, TierGold},
		{200_000, TierPlatinum},
	}
	for _, tc := range cases {
		if got := TierFor(tc.spend); got != tc.want {
			t.Errorf("TierFor(%d) = %s, want %s", tc.spend, got, tc.want)
		}
	}
}

func TestDiscountBps(t *testing.T) {
	cases := map[Tier]int32{TierStandard: 0, TierGold: 500, TierPlatinum: 1000}
	for tier, want := range cases {
		if got := tier.DiscountBps(); got != want {
			t.Errorf("%s.DiscountBps() = %d, want %d", tier, got, want)
		}
	}
}

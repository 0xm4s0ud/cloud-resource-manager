package domain

import "testing"

func TestAvailable(t *testing.T) {
	p := ResourcePool{Total: 1000, OversubscribeFactor: 1.0, DedicatedAmount: 600, ReservedAmount: 50}
	if got := p.Available(); got != 350 {
		t.Fatalf("Available() = %d, want 350", got)
	}
}

func TestAvailableRatio(t *testing.T) {
	r := AvailableRatio(100, 40, 10, 1.0)
	if r < 0.49 || r > 0.51 {
		t.Fatalf("AvailableRatio = %v, want ~0.5", r)
	}
}

func TestValidateTransition(t *testing.T) {
	cases := []struct {
		from, to RequestStatus
		ok       bool
	}{
		{StatusReserved, StatusCompleted, true},
		{StatusReserved, StatusCancelled, true},
		{StatusReserved, StatusExpired, true},
		{StatusReserved, StatusReserved, true},
		{StatusCompleted, StatusCancelled, false},
		{StatusRejected, StatusReserved, false},
	}
	for _, tc := range cases {
		err := ValidateTransition(tc.from, tc.to)
		if tc.ok && err != nil {
			t.Fatalf("%s -> %s: unexpected error %v", tc.from, tc.to, err)
		}
		if !tc.ok && err == nil {
			t.Fatalf("%s -> %s: expected error", tc.from, tc.to)
		}
	}
}

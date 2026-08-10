package domain

import "fmt"

func AvailableRatio(total, dedicated, reserved int64, oversubscribe float64) float64 {
	if total <= 0 {
		return 0
	}
	effective := float64(total) * oversubscribe
	avail := effective - float64(dedicated) - float64(reserved)
	if avail < 0 {
		avail = 0
	}
	return avail / float64(total)
}

func ValidateTransition(from, to RequestStatus) error {
	allowed := map[RequestStatus]map[RequestStatus]bool{
		StatusReserved: {
			StatusCompleted: true,
			StatusCancelled: true,
			StatusExpired:   true,
			StatusReserved:  true, // extend
		},
	}
	if m, ok := allowed[from]; ok && m[to] {
		return nil
	}
	return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, from, to)
}

var (
	ErrInvalidTransition = fmt.Errorf("invalid status transition")
	ErrNotImplemented    = fmt.Errorf("not implemented")

	ErrDuplicateRequest = fmt.Errorf("duplicate request")
)

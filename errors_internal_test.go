package quasar

import (
	"errors"
	"fmt"
	"testing"
)

func TestReattachNoLeader(t *testing.T) {
	t.Parallel()

	// transportReconstructed mimics how a transport rebuilds a remote
	// reply error: the leader serializes err.Error() into the response
	// and the requesting node turns it back into a plain string error
	// via errors.New (see transports/*.go), dropping sentinel identity.
	transportReconstructed := errors.New(
		fmt.Errorf("failed to get leader: %w: %w", ErrNoLeader, ErrNoLeader).Error(),
	)

	tests := []struct {
		name      string
		err       error
		wantNil   bool
		wantNoLdr bool
	}{
		{
			name:    "nil stays nil",
			err:     nil,
			wantNil: true,
		},
		{
			name:      "locally wrapped sentinel is preserved untouched",
			err:       fmt.Errorf("failed to get leader: %w: %w", ErrNoLeader, ErrNoLeader),
			wantNoLdr: true,
		},
		{
			name:      "transport-reconstructed no-leader regains the sentinel",
			err:       transportReconstructed,
			wantNoLdr: true,
		},
		{
			name:      "unrelated error is left alone",
			err:       errors.New("connection refused"),
			wantNoLdr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := reattachNoLeader(tc.err)
			if tc.wantNil {
				if got != nil {
					t.Fatalf("expected nil, got %v", got)
				}
				return
			}
			if errors.Is(got, ErrNoLeader) != tc.wantNoLdr {
				t.Fatalf("errors.Is(%q, ErrNoLeader) = %v, want %v",
					got, errors.Is(got, ErrNoLeader), tc.wantNoLdr)
			}
			// The original message must survive re-wrapping.
			if msg := tc.err.Error(); got.Error() == "" || got.Error()[:len(msg)] != msg {
				t.Fatalf("original message lost: got %q, want prefix %q", got.Error(), msg)
			}
		})
	}
}

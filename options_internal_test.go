package quasar

import (
	"testing"
	"time"
)

func TestWithNoLeaderTimeoutZeroKeepsDefault(t *testing.T) {
	cfg := getOptions([]Option{WithNoLeaderTimeout(0)})
	if cfg.noLeaderTimeout != defaultNoLeaderTimeout {
		t.Fatalf("WithNoLeaderTimeout(0) should keep the default %v, got %v",
			defaultNoLeaderTimeout, cfg.noLeaderTimeout)
	}

	cfg = getOptions([]Option{WithNoLeaderTimeout(-1)})
	if cfg.noLeaderTimeout != defaultNoLeaderTimeout {
		t.Fatalf("WithNoLeaderTimeout(<0) should keep the default %v, got %v",
			defaultNoLeaderTimeout, cfg.noLeaderTimeout)
	}

	const custom = 3 * time.Second
	cfg = getOptions([]Option{WithNoLeaderTimeout(custom)})
	if cfg.noLeaderTimeout != custom {
		t.Fatalf("WithNoLeaderTimeout(%v) should override, got %v", custom, cfg.noLeaderTimeout)
	}
}

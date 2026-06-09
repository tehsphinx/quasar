// Copyright (c) RealTyme SA. All rights reserved.

package quasar

import (
	"errors"
	"fmt"
	"strings"
)

// reattachNoLeader restores the ErrNoLeader sentinel on errors that lost
// it crossing a transport boundary. A leader's reply is serialized to a
// plain string and reconstructed on the requesting node via errors.New
// (see transports/*.go), which drops sentinel identity: a remote
// "no leader" failure then no longer satisfies errors.Is(err, ErrNoLeader),
// so callers that gate retries on it (e.g. pgwatcher, RT-13005) silently
// stop retrying even though the locally-detected case still works. When the
// reconstructed message still carries the ErrNoLeader marker, re-wrap it so
// errors.Is works regardless of where the condition was detected.
func reattachNoLeader(err error) error {
	if err == nil || errors.Is(err, ErrNoLeader) {
		return err
	}
	if strings.Contains(err.Error(), ErrNoLeader.Error()) {
		return fmt.Errorf("%w: %w", err, ErrNoLeader)
	}
	return err
}

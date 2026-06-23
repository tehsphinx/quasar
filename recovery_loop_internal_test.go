package quasar

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/raft"
)

// TestRecoveryLoopFlappingLeaderFires is the RT-13283 reproducer. A flapping
// link lets a leader form in brief blips, each shorter than the stability
// dwell. The recovery window must still elapse and fire, because a blip shorter
// than stableFor must not cancel (or reset) the recovery timer.
//
// Before the fix, every leader observation cancelled the recovery timer
// immediately, so a leader blip every few hundred ms reset the window forever
// and recovery never fired — exactly the production incident where a voter sat
// leaderless for ~4h until a manual restart.
func TestRecoveryLoopFlappingLeaderFires(t *testing.T) {
	const (
		after     = 200 * time.Millisecond
		stableFor = 80 * time.Millisecond
		blipOn    = 40 * time.Millisecond // < stableFor, so a blip never stabilizes
		blipOff   = 20 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var leader atomic.Bool // starts false: leaderless
	fired := make(chan struct{}, 1)
	fire := func() bool {
		select {
		case fired <- struct{}{}:
		default:
		}
		return true // pretend raft was swapped so the loop exits
	}

	chObs := make(chan raft.Observation, 8)
	go recoveryLoop(ctx, context.Background(), chObs, leader.Load, fire, after, stableFor)

	flapStop := make(chan struct{})
	go func() {
		for {
			select {
			case <-flapStop:
				return
			default:
			}
			leader.Store(true)
			chObs <- raft.Observation{}
			time.Sleep(blipOn)
			leader.Store(false)
			chObs <- raft.Observation{}
			time.Sleep(blipOff)
		}
	}()
	defer close(flapStop)

	select {
	case <-fired:
		// good: recovery fired despite the flapping leader.
	case <-time.After(2 * time.Second):
		t.Fatal("recovery never fired despite sustained leaderlessness — flapping leader observations reset the window (RT-13283)")
	}
}

// TestRecoveryLoopStableLeaderCancels guards the cancel path: a leader that
// persists beyond stableFor must cancel the armed recovery timer so a healthy
// cluster is never torn down by a spurious recovery.
func TestRecoveryLoopStableLeaderCancels(t *testing.T) {
	const (
		after     = 200 * time.Millisecond
		stableFor = 60 * time.Millisecond
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var leader atomic.Bool // starts false: seed arms the recovery timer
	fired := make(chan struct{}, 1)
	fire := func() bool {
		select {
		case fired <- struct{}{}:
		default:
		}
		return true
	}

	chObs := make(chan raft.Observation, 4)
	go recoveryLoop(ctx, context.Background(), chObs, leader.Load, fire, after, stableFor)

	// A stable leader appears shortly after start and stays.
	time.Sleep(20 * time.Millisecond)
	leader.Store(true)
	chObs <- raft.Observation{}

	select {
	case <-fired:
		t.Fatal("recovery fired despite a stable leader holding past the dwell")
	case <-time.After(after + 200*time.Millisecond):
		// good: the stable leader cancelled recovery before the window elapsed.
	}
}

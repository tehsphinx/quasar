// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
)

// TestNatsPersistedConsumer_TerminalDeliveryLogged is the RT-13042 M19
// regression test. With a single shared durable, a Nack ping-pong between an
// outgoing and incoming leader's sessions burns delivery attempts; once
// NumDelivered reaches MaxDeliver, the WorkQueue stream (no DLQ) would
// silently drop the "persisted" write on the next Nack. consume() must detect
// the terminal delivery and log loudly at Error level so it is not lost in
// silence.
func TestNatsPersistedConsumer_TerminalDeliveryLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{Output: &buf, Level: hclog.Error})

	q := &natsPersistedQueue{ackWait: time.Second, maxDeliver: 16, logger: logger}

	pullCtx, cancel := context.WithCancel(context.Background())
	// Cancel up front so consume returns immediately after the terminal-
	// delivery check instead of blocking on the items channel.
	cancel()
	c := &natsPersistedConsumer{
		queue: q,
		items: make(chan PersistedItem),
		mctx:  noopMessagesContext{ctx: pullCtx},
	}

	msg := &fakeJSMsg{numDelivered: uint64(q.maxDeliver)}
	c.consume(pullCtx, msg)

	out := buf.String()
	if !strings.Contains(out, "terminal delivery") {
		t.Fatalf("expected terminal-delivery Error log, got: %q", out)
	}
}

// TestNatsPersistedConsumer_NonTerminalDeliveryNotLogged confirms the log only
// fires once redelivery is genuinely exhausted, not on every delivery.
func TestNatsPersistedConsumer_NonTerminalDeliveryNotLogged(t *testing.T) {
	var buf bytes.Buffer
	logger := hclog.New(&hclog.LoggerOptions{Output: &buf, Level: hclog.Error})

	q := &natsPersistedQueue{ackWait: time.Second, maxDeliver: 16, logger: logger}

	pullCtx, cancel := context.WithCancel(context.Background())
	cancel()
	c := &natsPersistedConsumer{
		queue: q,
		items: make(chan PersistedItem),
		mctx:  noopMessagesContext{ctx: pullCtx},
	}

	msg := &fakeJSMsg{numDelivered: 1}
	c.consume(pullCtx, msg)

	if out := buf.String(); strings.Contains(out, "terminal delivery") {
		t.Fatalf("unexpected terminal-delivery log on first delivery: %q", out)
	}
}

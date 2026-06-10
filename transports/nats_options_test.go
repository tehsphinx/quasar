// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"bytes"
	"strings"
	"testing"

	"github.com/hashicorp/go-hclog"
)

// TestWithNATSLogOutputWiresDefaultLogger is the RT-13042 S7 regression test.
// WithNATSLogOutput stored cfg.output, but getNATSOptions built the default
// logger with hclog.DefaultOutput and never read cfg.output — so the option
// was silently ignored and nothing reached the supplied writer. Pre-fix the
// buffer stays empty; post-fix the default logger writes to it.
func TestWithNATSLogOutputWiresDefaultLogger(t *testing.T) {
	var buf bytes.Buffer

	cfg := getNATSOptions([]NATSOption{WithNATSLogOutput(&buf)})
	cfg.logger.Info("hello from quasar")

	if got := buf.String(); !strings.Contains(got, "hello from quasar") {
		t.Fatalf("WithNATSLogOutput was ignored: log output buffer = %q", got)
	}
}

// TestWithNATSLoggerIgnoresLogOutput documents the precedence stated on
// WithNATSLogger: an explicit logger takes over, so WithNATSLogOutput must
// not be considered (the writer stays untouched by the default logger path).
func TestWithNATSLoggerIgnoresLogOutput(t *testing.T) {
	var explicit, output bytes.Buffer

	logger := hclog.New(&hclog.LoggerOptions{Output: &explicit})
	cfg := getNATSOptions([]NATSOption{
		WithNATSLogger(logger),
		WithNATSLogOutput(&output),
	})
	cfg.logger.Info("routed to explicit logger")

	if output.Len() != 0 {
		t.Fatalf("WithNATSLogOutput should be ignored when WithNATSLogger is set, got %q", output.String())
	}
	if got := explicit.String(); !strings.Contains(got, "routed to explicit logger") {
		t.Fatalf("explicit logger did not receive the message, got %q", got)
	}
}

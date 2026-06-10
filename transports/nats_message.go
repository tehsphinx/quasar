// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"errors"
	"fmt"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// multipartAssemblyTimeout bounds how long an in-progress multipart assembly
// may sit without receiving a new part before it is considered abandoned
// (the sender died mid-request) and evicted on the next message.
const multipartAssemblyTimeout = 30 * time.Second

// Message is one in-progress multipart assembly.
type Message struct {
	requestID string
	data      []byte
	lastID    int
	updatedAt time.Time
}

func (m *Message) Append(b []byte) {
	m.data = append(m.data, b...)
	m.lastID++
	m.updatedAt = time.Now()
}

func (s *NATSTransport) publishMultiPart(subj string, data []byte) ([]byte, string, int, error) {
	var (
		partID    int
		requestID = atomic.AddUint64(&s.requestIDCounter, 1)
		// Qualify the request ID with the sender name: the counter alone
		// starts at 0 on every transport instance, so two nodes forwarding
		// large Stores to the same leader subject concurrently would collide
		// on the same ID and corrupt each other's reassembly (RT-13042 M3).
		requestIDStr = s.serverName + "/" + strconv.FormatUint(requestID, base10)
	)
	for {
		partID++
		part := data[:min(len(data), s.maxMsgSize)]
		data = data[len(part):]
		if len(data) == 0 {
			return part, requestIDStr, partID, nil
		}

		msg := nats.NewMsg(subj)
		msg.Data = part
		msg.Header.Set("request_id", requestIDStr)
		msg.Header.Set("pkg_part", strconv.Itoa(partID))

		// Publish the part (no response expected)
		if err := s.conn.PublishMsg(msg); err != nil {
			return nil, "", 0, fmt.Errorf("failed to publish part %d: %w", partID, err)
		}
	}
}

func newMultipartAssembler() *multipartAssembler {
	return &multipartAssembler{
		messages: map[string]*Message{},
	}
}

// multipartAssembler reassembles multipart messages, keyed by the
// sender-qualified request_id header. Keying by request ID isolates
// concurrently interleaved senders from each other: any node (and multiple
// goroutines per node) may forward large Stores to the same leader subject,
// so a single shared buffer would mix parts from different requests
// (RT-13042 M3). Not safe for concurrent use — NATS delivers the callbacks
// of one subscription serially, which is the only context this runs in.
type multipartAssembler struct {
	messages map[string]*Message
}

// handle processes one incoming message. It returns the complete payload and
// true once a message is fully assembled; (nil, false, nil) for a middle part
// of an ongoing assembly.
func (a *multipartAssembler) handle(msg *nats.Msg) ([]byte, bool, error) {
	a.evictStale()

	hRequestID := msg.Header.Get("request_id")
	if hRequestID == "" {
		// A headerless message is a complete standalone request; it never
		// participates in any keyed assembly.
		return msg.Data, true, nil
	}

	message, ok := a.messages[hRequestID]
	if !ok {
		message = &Message{requestID: hRequestID}
		a.messages[hRequestID] = message
	}

	if hPkgPart := msg.Header.Get("pkg_part"); hPkgPart != strconv.Itoa(message.lastID+1) {
		delete(a.messages, hRequestID)
		return nil, false, errors.New("pkg_part mismatch")
	}

	message.Append(msg.Data)
	if msg.Reply == "" {
		// Middle part: the final part arrives as a request (with reply).
		return nil, false, nil
	}

	delete(a.messages, hRequestID)
	return message.data, true, nil
}

// evictStale drops assemblies that have not seen a new part within
// multipartAssemblyTimeout, so senders that die mid-request cannot
// accumulate abandoned buffers.
func (a *multipartAssembler) evictStale() {
	cutoff := time.Now().Add(-multipartAssemblyTimeout)
	for id, message := range a.messages {
		if message.updatedAt.Before(cutoff) {
			delete(a.messages, id)
		}
	}
}

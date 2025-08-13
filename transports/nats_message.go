// Copyright (c) RealTyme SA. All rights reserved.

package transports

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"sync/atomic"

	"github.com/nats-io/nats.go"
)

type Message struct {
	requestID string
	data      []byte
	lastID    int
}

func (m *Message) Append(b []byte) {
	m.data = append(m.data, b...)
	m.lastID++
}

func (m *Message) Reset() {
	m.requestID = ""
	m.data = m.data[:0]
	m.lastID = 0
}

func (m *Message) GetDataAndReset() []byte {
	data := slices.Clone(m.data)
	m.Reset()
	return data
}

func (m *Message) IsZero() bool {
	return m.requestID == "" && m.lastID == 0
}

func (s *NATSTransport) publishMultiPart(subj string, data []byte) ([]byte, string, int, error) {
	var (
		partID       int
		requestID    = atomic.AddUint64(&s.requestIDCounter, 1)
		requestIDStr = strconv.FormatUint(requestID, base10)
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

// handleMultiPart handles multipart messages. It returns true if the message was handled and handler can return.
func handleMultiPart(msg *nats.Msg, message *Message) (bool, error) {
	if hRequestID := msg.Header.Get("request_id"); hRequestID != "" {
		hPkgPart := msg.Header.Get("pkg_part")
		if message.IsZero() {
			message.requestID = hRequestID
		} else if hRequestID != message.requestID {
			message.Reset()
			return false, errors.New("request_id mismatch")
		}
		if hPkgPart != strconv.Itoa(message.lastID+1) {
			message.Reset()
			return false, errors.New("pkg_part mismatch")
		}
	}

	message.Append(msg.Data)
	return msg.Reply != "", nil
}

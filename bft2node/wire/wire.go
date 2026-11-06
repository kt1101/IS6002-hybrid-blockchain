// Package wire is the local API between bft2-node and the veritas server
// on the same container (consensus/bft2 in the main module is the client;
// it keeps its own copy of this codec, the two must stay in sync).
//
// HTTP over TCP on 127.0.0.1 (default port 7201):
//
//	POST /submit   body = one command (one marshalled Veritas block), 1..max
//	               bytes. 200: accepted (queued for proposal on this replica);
//	               400 empty body, 413 too large, 503 queue full or stopping,
//	               500 other; the body of an error reply is a text reason.
//	POST /forward  (replica-to-replica, port 7202 in the cluster) body = one
//	               command another replica's /submit received; queued here
//	               for proposal, not forwarded further. Same replies as
//	               /submit (an already known command: 200).
//	GET  /deliver  200 and a never-ending body of frames, one per committed
//	               command in commit order (empty filler blocks are never
//	               sent). One subscriber at a time: a second one gets 409.
//
// A frame is an 8-byte big-endian sequence number (0 for the first committed
// command of this replica, then +1), a 4-byte big-endian payload length and
// the payload (the exact command bytes).
package wire

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	// SubmitPath and DeliverPath are the two endpoints of the local API.
	SubmitPath  = "/submit"
	DeliverPath = "/deliver"
	ForwardPath = "/forward"
	// HeaderLen is the size of a frame header (seq + length).
	HeaderLen = 12
	// MaxFrame bounds the payload a reader accepts by default (64 MiB).
	MaxFrame = 64 << 20
)

// ErrFrameTooLarge is returned by ReadFrame for a payload over its limit.
var ErrFrameTooLarge = errors.New("wire: frame too large")

// AppendHeader appends the header of a frame (seq, n-byte payload) to dst.
func AppendHeader(dst []byte, seq uint64, n int) []byte {
	var h [HeaderLen]byte
	binary.BigEndian.PutUint64(h[0:8], seq)
	binary.BigEndian.PutUint32(h[8:12], uint32(n))
	return append(dst, h[:]...)
}

// AppendFrame appends the frame (seq, payload) to dst and returns it.
func AppendFrame(dst []byte, seq uint64, payload []byte) []byte {
	return append(AppendHeader(dst, seq, len(payload)), payload...)
}

// ReadFrame reads one frame. It returns io.EOF only when r ends cleanly
// before a frame, io.ErrUnexpectedEOF for a truncated frame, and
// ErrFrameTooLarge (without reading the payload) for a payload over max.
// The payload is a fresh slice.
func ReadFrame(r io.Reader, max int) (seq uint64, payload []byte, err error) {
	var h [HeaderLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	seq = binary.BigEndian.Uint64(h[0:8])
	n := binary.BigEndian.Uint32(h[8:12])
	if uint64(n) > uint64(max) {
		return 0, nil, fmt.Errorf("%w: %d bytes (max %d)", ErrFrameTooLarge, n, max)
	}
	payload = make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		return 0, nil, err
	}
	return seq, payload, nil
}

package bft2

// Copy of the local API codec of bft2-node (bft2node/wire/wire.go, a
// separate Go module); keep the two in sync.
//
//	POST /submit   body = one Veritas block. 200 accepted; 400 empty,
//	               413 too large, 503 queue full or stopping, 500 other.
//	GET  /deliver  200, then frames: 8-byte big-endian seq (0, 1, ...),
//	               4-byte big-endian length, payload (the exact block).

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	submitPath  = "/submit"
	deliverPath = "/deliver"
	headerLen   = 12
)

// appendFrame appends the frame (seq, payload) to dst (tests).
func appendFrame(dst []byte, seq uint64, payload []byte) []byte {
	var h [headerLen]byte
	binary.BigEndian.PutUint64(h[0:8], seq)
	binary.BigEndian.PutUint32(h[8:12], uint32(len(payload)))
	dst = append(dst, h[:]...)
	return append(dst, payload...)
}

// readFrame reads one frame; the payload is a fresh slice. io.EOF means the
// stream ended cleanly between frames.
func readFrame(r io.Reader, max int) (seq uint64, payload []byte, err error) {
	var h [headerLen]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return 0, nil, err
	}
	seq = binary.BigEndian.Uint64(h[0:8])
	n := binary.BigEndian.Uint32(h[8:12])
	if uint64(n) > uint64(max) {
		return 0, nil, fmt.Errorf("frame of %d bytes over the %d-byte limit", n, max)
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

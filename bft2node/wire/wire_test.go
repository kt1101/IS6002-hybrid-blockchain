package wire

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestFrameRoundTrip(t *testing.T) {
	payloads := [][]byte{{0x00}, []byte("veritas block"), bytes.Repeat([]byte{0xff, 0x00, 0x7f}, 1000)}
	var buf []byte
	for i, p := range payloads {
		buf = AppendFrame(buf, uint64(i+41), p)
	}
	r := bytes.NewReader(buf)
	for i, want := range payloads {
		seq, got, err := ReadFrame(r, MaxFrame)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if seq != uint64(i+41) || !bytes.Equal(got, want) {
			t.Fatalf("frame %d = (%d, %d bytes), want (%d, %d bytes)", i, seq, len(got), i+41, len(want))
		}
	}
	if _, _, err := ReadFrame(r, MaxFrame); err != io.EOF {
		t.Fatalf("after the last frame: err = %v, want io.EOF", err)
	}
}

func TestReadFrameRejectsOversizedAndTruncated(t *testing.T) {
	big := AppendFrame(nil, 0, make([]byte, 100))
	if _, _, err := ReadFrame(bytes.NewReader(big), 99); !errors.Is(err, ErrFrameTooLarge) {
		t.Fatalf("oversized frame: err = %v, want ErrFrameTooLarge", err)
	}
	if _, _, err := ReadFrame(bytes.NewReader(big[:HeaderLen+50]), MaxFrame); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated payload: err = %v, want io.ErrUnexpectedEOF", err)
	}
	if _, _, err := ReadFrame(bytes.NewReader(big[:5]), MaxFrame); err != io.ErrUnexpectedEOF {
		t.Fatalf("truncated header: err = %v, want io.ErrUnexpectedEOF", err)
	}
}

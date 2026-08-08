package sync

import (
	"testing"
	"time"
)

func TestComputeOffset(t *testing.T) {
	clientSend := time.Unix(1_700_000_000, 0)
	serverRecv := time.Unix(1_700_000_010, 0)
	serverSend := time.Unix(1_700_000_020, 0)
	clientRecv := time.Unix(1_700_000_010, 0)

	offset := ComputeOffset(clientSend, serverRecv, serverSend, clientRecv)
	if offset != 10*time.Second {
		t.Fatalf("expected offset 10s, got %v", offset)
	}
}

func TestConvertSharedTimeToLocalTime(t *testing.T) {
	shared := time.Unix(1_700_000_500, 0)
	offset := 10 * time.Second

	local := ConvertSharedTimeToLocal(shared, offset)
	if local != time.Unix(1_700_000_490, 0) {
		t.Fatalf("expected local time 1_700_000_490, got %v", local)
	}
}

func TestNextPlaybackTime(t *testing.T) {
	deadline := time.Unix(1_700_000_400, 0)

	next := NextPlaybackTime(deadline, 2*time.Second)
	if next != deadline.Add(2*time.Second) {
		t.Fatalf("expected next time to be deadline + 2s, got %v", next)
	}
}

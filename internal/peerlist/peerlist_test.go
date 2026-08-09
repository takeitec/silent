package peerlist

import (
	"testing"

	"silent/internal/models"
)

func TestResetRemovesAllPeers(t *testing.T) {
	pl := New()
	pl.Add("peer-a", "10.0.0.1", models.RoleFollower, 0)
	pl.Add("peer-b", "10.0.0.2", models.RoleLeader, 0)

	if got := len(pl.Peers()); got != 2 {
		t.Fatalf("expected 2 peers before reset, got %d", got)
	}

	pl.Reset()

	if got := len(pl.Peers()); got != 0 {
		t.Fatalf("expected no peers after reset, got %d", got)
	}
}

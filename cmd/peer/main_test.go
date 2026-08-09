package main

import (
	"flag"
	"os"
	"testing"

	"silent/internal/models"
)

func TestParseFlagsRoomDefaultsToTrue(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	os.Args = []string{"peer"}

	cfg := parseFlags()
	if !cfg.room {
		t.Fatalf("expected room mode to default to true")
	}
}

func TestParseFlagsRoomCanBeDisabled(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	os.Args = []string{"peer", "-room=false"}

	cfg := parseFlags()
	if cfg.room {
		t.Fatalf("expected room mode to be disabled when flag is false")
	}
}

func TestShouldProbeLeaderSkipsLeaderAndMissingLeader(t *testing.T) {
	cfg := config{leader: true}
	if shouldProbeLeader(cfg, &models.Peer{ID: "leader"}) {
		t.Fatal("expected leader to skip probing")
	}

	if shouldProbeLeader(config{}, nil) {
		t.Fatal("expected missing leader to skip probing")
	}
}

func TestShouldProbeLeaderAllowsFollowerToProbe(t *testing.T) {
	leader := &models.Peer{ID: "leader"}
	cfg := config{}
	if !shouldProbeLeader(cfg, leader) {
		t.Fatal("expected follower to probe a discovered leader")
	}
}

func TestEffectiveControlPortUsesSharedPortInRoomMode(t *testing.T) {
	cfg := config{port: 9999, room: true}
	if got := effectiveControlPort(cfg); got != 9999 {
		t.Fatalf("expected shared control port 9999 in room mode, got %d", got)
	}
}

func TestEffectiveControlPortUsesPortPlusOneOutsideRoomMode(t *testing.T) {
	cfg := config{port: 9999, room: false}
	if got := effectiveControlPort(cfg); got != 10000 {
		t.Fatalf("expected default control port 10000 outside room mode, got %d", got)
	}
}

func TestPeerControlPortPrefersExplicitControlPort(t *testing.T) {
	peer := models.Peer{Address: "192.0.2.10:50051", ControlPort: 10000}
	if got := peerControlPort(peer, 10001); got != 10000 {
		t.Fatalf("expected explicit control port 10000, got %d", got)
	}
}

func TestPeerControlPortFallsBackToConfiguredPort(t *testing.T) {
	peer := models.Peer{Address: "192.0.2.10:10000"}
	if got := peerControlPort(peer, 10001); got != 10001 {
		t.Fatalf("expected fallback control port 10001, got %d", got)
	}
}

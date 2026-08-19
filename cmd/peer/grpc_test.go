package main

import (
	"context"
	"errors"
	"net"
	"silent/internal/models"
	"silent/internal/peerlist"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

func TestBeginSessionRejectsDuplicateUntilLeaseExpires(t *testing.T) {
	srv := &peerControlServer{}

	if !srv.beginSession("demo-session") {
		t.Fatalf("expected first session start to succeed")
	}

	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to be rejected")
	}

	srv.finishSession("demo-session")
	if !srv.beginSession("demo-session") {
		t.Fatalf("expected session to be accepted again after finish")
	}
}

func TestBeginSessionUsesNormalisedSessionKey(t *testing.T) {
	srv := &peerControlServer{}

	if !srv.beginSession("  demo-session  ") {
		t.Fatalf("expected first session start to succeed")
	}

	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to be rejected when spacing differs")
	}
}

func TestBeginSessionExpiresAfterLease(t *testing.T) {
	originalLease := sessionLease
	defer func() { sessionLease = originalLease }()

	sessionLease = 10 * time.Millisecond
	srv := &peerControlServer{}

	if !srv.beginSession("demo-session") {
		t.Fatalf("expected first session start to succeed")
	}

	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to be rejected before lease expires")
	}

	time.Sleep(20 * time.Millisecond)
	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to remain rejected until sweep runs")
	}

	srv.sessions().Sweep(time.Now(), time.Minute)
	if !srv.beginSession("demo-session") {
		t.Fatalf("expected session to be accepted again after sweep marks lease-expired session terminal")
	}
}

func TestBeginSessionRejectsDuplicateWhenActiveCancelExists(t *testing.T) {
	originalLease := sessionLease
	defer func() { sessionLease = originalLease }()

	sessionLease = 10 * time.Millisecond
	srv := &peerControlServer{}

	if !srv.beginSession("demo-session") {
		t.Fatalf("expected first session start to succeed")
	}

	srv.sessions().Transition("demo-session", sessionActive, nil)

	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to be rejected while active")
	}
}

func TestPeerTargetUsesAdvertisedPort(t *testing.T) {
	got := peerTarget("192.168.1.10:50052", 50051)
	if got != "192.168.1.10:50052" {
		t.Fatalf("expected advertised port to be preserved, got %q", got)
	}
}

func TestLeaderTargetSkipsSelf(t *testing.T) {
	srv := &peerControlServer{id: "follower-A", pl: peerlist.New(), grpcPort: 50052}
	srv.pl.Add("follower-A", "127.0.0.1:50052", models.RoleFollower, 0)

	if got := srv.leaderTarget(); got != "" {
		t.Fatalf("expected self leader target to be rejected, got %q", got)
	}
}

func TestStreamTargetUsesPeerAddress(t *testing.T) {
	addr := &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 40000}
	ctx := peer.NewContext(context.Background(), &peer.Peer{Addr: addr})

	if got := streamTarget(ctx); got != addr.String() {
		t.Fatalf("expected peer address %q, got %q", addr.String(), got)
	}
}

func TestStreamTargetUsesMetadataFallback(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-peer-address", "10.0.0.1:50051"))

	if got := streamTarget(ctx); got != "10.0.0.1:50051" {
		t.Fatalf("expected metadata address, got %q", got)
	}
}

func TestClassifyDisconnect(t *testing.T) {
	retriable := status.Errorf(codes.ResourceExhausted, "slow subscriber dropped=200 window=5s")
	if d := classifyDisconnect(retriable); !d.retry {
		t.Fatalf("expected ResourceExhausted to be retriable, got %+v", d)
	}

	unavailable := status.Errorf(codes.Unavailable, "connection reset")
	if d := classifyDisconnect(unavailable); !d.retry {
		t.Fatalf("expected Unavailable to be retriable, got %+v", d)
	}

	notFound := status.Errorf(codes.NotFound, "no leader session %q", "demo-session")
	if d := classifyDisconnect(notFound); d.retry {
		t.Fatalf("expected NotFound to be terminal (leader restarted), got %+v", d)
	}

	canceled := status.Errorf(codes.Canceled, "context canceled")
	if d := classifyDisconnect(canceled); d.retry {
		t.Fatalf("expected Canceled to be terminal, got %+v", d)
	}

	unrelated := errors.New("some unrelated plain error")
	if d := classifyDisconnect(unrelated); d.retry {
		t.Fatalf("expected an unclassified plain error to be treated as non-retriable, got %+v", d)
	}
}

func TestShouldLogChunkIgnoresVariableSizeMismatchInMilestoneMode(t *testing.T) {
	srv := &peerControlServer{chunkLogEvery: 100}

	if srv.shouldLogChunk(chunkLogModeMilestone, 1, 744, 3840, "opus") {
		t.Fatal("expected variable-size Opus payload mismatch to be ignored in milestone mode")
	}
}

func TestShouldLogChunkFlagsFixedSizeMismatchInMilestoneMode(t *testing.T) {
	srv := &peerControlServer{chunkLogEvery: 100}

	if !srv.shouldLogChunk(chunkLogModeMilestone, 1, 3000, 3840, "pcm") {
		t.Fatal("expected fixed-size PCM mismatch to trigger a milestone log")
	}
}

package main

import (
	"context"
	"net"
	"silent/internal/models"
	"silent/internal/peerlist"
	"testing"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
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
	if !srv.beginSession("demo-session") {
		t.Fatalf("expected session to be accepted again after lease expiry")
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

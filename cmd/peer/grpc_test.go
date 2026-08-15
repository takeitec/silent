package main

import (
	"context"
	"errors"
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
	if !srv.beginSession("demo-session") {
		t.Fatalf("expected session to be accepted again after lease expiry")
	}
}

func TestBeginSessionRejectsDuplicateWhenActiveCancelExists(t *testing.T) {
	originalLease := sessionLease
	defer func() { sessionLease = originalLease }()

	sessionLease = 10 * time.Millisecond
	srv := &peerControlServer{
		sessionCancels: map[string]context.CancelFunc{
			"demo-session": func() {},
		},
	}

	if srv.beginSession("demo-session") {
		t.Fatalf("expected duplicate session start to be rejected while active cancel exists")
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

func TestIsSlowSubscriberDisconnect(t *testing.T) {
	if !isSlowSubscriberDisconnect(errors.New("slow subscriber dropped=200 window=5s")) {
		t.Fatal("expected slow subscriber error to be rejoinable")
	}
	if isSlowSubscriberDisconnect(errors.New("rpc error: code = Unavailable desc = connection reset")) {
		t.Fatal("did not expect unrelated transport error to be rejoinable")
	}
}

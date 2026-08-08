package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"silent/internal/control"
	"silent/internal/models"
	"silent/internal/peerlist"
	"silent/internal/sync"
)

type peerControlServer struct {
	control.UnimplementedPeerControlServer

	id       string
	isLeader bool
	pl       *peerlist.PeerList
	grpcPort int
	wavPath  string
	offsetCh chan time.Duration
}

func (s *peerControlServer) StartPlayback(ctx context.Context, req *control.PlaybackRequest) (*control.PlaybackResponse, error) {
	log.Printf("gRPC: StartPlayback received audio_id=%q audio_path=%q", req.AudioId, req.AudioPath)

	if !s.isLeader {
		log.Printf("gRPC: StartPlayback rejected because this peer is not the leader")
		return &control.PlaybackResponse{Accepted: false, Message: "not leader"}, nil
	}

	sharedAt := time.Now().Add(3 * time.Second)
	peers := s.pl.Peers()

	log.Printf("gRPC: leader will notify %d follower(s) at shared time %s", len(peers), sharedAt.Format(time.RFC3339Nano))

	for _, p := range peers {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		target := fmt.Sprintf("%s:%d", hostFromAddress(p.Address), s.grpcPort)
		log.Printf("gRPC: notifying peer %s at %s", p.ID, target)

		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("gRPC: notify %s failed to connect: %v", p.ID, err)
			continue
		}

		client := control.NewPeerControlClient(conn)
		_, err = client.NotifyPlayback(ctx, &control.PlaybackCommand{
			AudioId:       req.AudioId,
			AudioPath:     req.AudioPath,
			SharedAtNanos: sharedAt.UnixNano(),
		})
		conn.Close()

		if err != nil {
			log.Printf("gRPC: notify playback to %s failed: %v", p.ID, err)
			continue
		}

		log.Printf("gRPC: notify playback to %s succeeded", p.ID)
	}

	return &control.PlaybackResponse{Accepted: true, Message: "playback started"}, nil
}

func (s *peerControlServer) NotifyPlayback(ctx context.Context, req *control.PlaybackCommand) (*control.PlaybackAck, error) {
	sharedAt := time.Unix(0, req.SharedAtNanos)
	var offset time.Duration

	select {
	case currentOffset := <-s.offsetCh:
		offset = currentOffset
	default:
	}

	localAt := sync.ConvertSharedTimeToLocal(sharedAt, offset)

	log.Printf("received playback command for %s at local %s", req.AudioId, localAt.Format(time.RFC3339Nano))

	go schedulePlayback(localAt, req.AudioPath)

	return &control.PlaybackAck{Accepted: true}, nil
}

func hostFromAddress(addr string) string {
	host := strings.TrimSpace(addr)

	if strings.Contains(host, "@") {
		parts := strings.SplitN(host, "@", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
			host = strings.TrimSpace(parts[1])
		}
	}

	if h, _, err := net.SplitHostPort(host); err == nil {
		return h
	}

	return host
}

func startGRPCServer(addr string, srv *peerControlServer) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	gs := grpc.NewServer()
	control.RegisterPeerControlServer(gs, srv)

	log.Printf("gRPC server listening on %s", addr)

	return gs.Serve(lis)
}

func triggerPlaybackOnLeader(addr string) error {
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	_, err = client.StartPlayback(context.Background(), &control.PlaybackRequest{
		AudioId:   "demo",
		AudioPath: "demo.wav",
	})
	return err
}

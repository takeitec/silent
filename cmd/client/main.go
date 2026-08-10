package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"silent/internal/control"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}

	command := os.Args[1]
	args := os.Args[2:]

	fs := flag.NewFlagSet("client", flag.ExitOnError)
	leaderAddr := fs.String("leader-addr", "127.0.0.1:50051", "leader gRPC address")
	audioID := fs.String("audio-id", "demo", "audio id")
	audioPath := fs.String("audio-path", "demo.wav", "audio path")
	sessionID := fs.String("session-id", fmt.Sprintf("session-%d", time.Now().Unix()), "stream session id")
	followerID := fs.String("follower-id", "", "specific follower peer id for late join (optional)")
	sharedAt := fs.Int64("shared-at-nanos", time.Now().Add(3*time.Second).UnixNano(), "shared playback time in nanoseconds")

	if err := fs.Parse(args); err != nil {
		log.Fatalf("parse flags: %v", err)
	}

	conn, err := grpc.NewClient(*leaderAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)

	switch command {
	case "play":
		resp, err := client.StartPlayback(context.Background(), &control.PlaybackRequest{
			AudioId:   *audioID,
			AudioPath: *audioPath,
		})
		if err != nil {
			log.Fatalf("start playback failed: %v", err)
		}
		log.Printf("play accepted=%v message=%s", resp.Accepted, resp.Message)

	case "stream":
		resp, err := client.StartStreamPlayback(context.Background(), &control.StreamPlaybackRequest{
			SessionId:     *sessionID,
			AudioId:       *audioID,
			AudioPath:     *audioPath,
			SharedAtNanos: *sharedAt,
		})
		if err != nil {
			log.Fatalf("start stream playback failed: %v", err)
		}
		log.Printf("stream accepted=%v session=%s message=%s", resp.Accepted, resp.SessionId, resp.Message)

	case "stop-stream":
		resp, err := client.StopStreamPlayback(context.Background(), &control.StopStreamRequest{
			SessionId: *sessionID,
			Reason:    "client stop request",
		})
		if err != nil {
			log.Fatalf("stop stream playback failed: %v", err)
		}
		log.Printf("stop-stream accepted=%v session=%s message=%s", resp.Accepted, resp.SessionId, resp.Message)

	case "join-stream":
		resp, err := client.JoinStreamPlayback(context.Background(), &control.JoinStreamRequest{
			SessionId:     *sessionID,
			FollowerId:    *followerID,
			SharedAtNanos: *sharedAt,
		})
		if err != nil {
			log.Fatalf("join stream playback failed: %v", err)
		}
		log.Printf("join-stream accepted=%v session=%s attempted=%d succeeded=%d message=%s", resp.Accepted, resp.SessionId, resp.Attempted, resp.Succeeded, resp.Message)

	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Println("usage:")
	fmt.Println("  client play [flags]")
	fmt.Println("  client stream [flags]")
	fmt.Println("  client join-stream [flags]")
	fmt.Println("  client stop-stream [flags]")
	fmt.Println("")
	fmt.Println("flags:")
	fmt.Println("  -leader-addr string")
	fmt.Println("  -audio-id string")
	fmt.Println("  -audio-path string")
	fmt.Println("  -session-id string")
	fmt.Println("  -follower-id string")
	fmt.Println("  -shared-at-nanos int")
}

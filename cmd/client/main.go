package main

import (
	"context"
	"flag"
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"silent/internal/control"
)

func main() {
	addr := flag.String("leader-addr", "127.0.0.1:50051", "leader gRPC address")
	audioID := flag.String("audio-id", "demo", "audio id")
	audioPath := flag.String("audio-path", "demo.wav", "audio path")
	flag.Parse()

	conn, err := grpc.NewClient(*addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("dial leader: %v", err)
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)

	resp, err := client.StartPlayback(context.Background(), &control.PlaybackRequest{
		AudioId:   *audioID,
		AudioPath: *audioPath,
	})
	if err != nil {
		log.Fatalf("start playback failed: %v", err)
	}

	log.Printf("accepted=%v message=%s", resp.Accepted, resp.Message)
}

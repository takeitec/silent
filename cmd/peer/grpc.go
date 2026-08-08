package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
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

func (s *peerControlServer) StartStreamPlayback(ctx context.Context, req *control.StreamPlaybackRequest) (*control.StreamPlaybackResponse, error) {
	log.Printf("gRPC stream: StartStreamPlayback session=%q audio_id=%q audio_path=%q shared_at=%s", req.SessionId, req.AudioId, req.AudioPath, time.Unix(0, req.SharedAtNanos).Format(time.RFC3339Nano))

	if !s.isLeader {
		target := s.leaderTarget()
		sharedAt := time.Unix(0, req.SharedAtNanos)

		if err := s.receiveAudioFromLeader(ctx, target, req.SessionId, req.AudioId, req.AudioPath, sharedAt); err != nil {
			log.Printf("gRPC stream: follower failed to receive session=%q from leader: %v", req.SessionId, err)
			return &control.StreamPlaybackResponse{
				Accepted: false,
				Message:  err.Error(),
			}, nil
		}

		return &control.StreamPlaybackResponse{
			Accepted:  true,
			SessionId: req.SessionId,
			Message:   "stream playback started",
		}, nil
	}

	// Leader path: tell each follower to connect to the leader and receive the stream.
	peers := s.pl.Peers()
	log.Printf("gRPC stream: leader starting stream session=%q for %d follower(s)", req.SessionId, len(peers))

	for _, p := range peers {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		target := fmt.Sprintf("%s:%d", hostFromAddress(p.Address), s.grpcPort)
		log.Printf("gRPC stream: telling follower=%s to receive from leader session=%q", p.ID, req.SessionId)

		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			log.Printf("gRPC stream: failed to contact follower=%s: %v", p.ID, err)
			continue
		}

		client := control.NewPeerControlClient(conn)
		_, err = client.StartStreamPlayback(ctx, req)
		conn.Close()

		if err != nil {
			log.Printf("gRPC stream: follower=%s rejected stream start: %v", p.ID, err)
			continue
		}

		log.Printf("gRPC stream: follower=%s accepted stream start", p.ID)
	}

	return &control.StreamPlaybackResponse{
		Accepted:  true,
		SessionId: req.SessionId,
		Message:   "stream playback started",
	}, nil
}

func (s *peerControlServer) StreamAudio(req *control.StreamPlaybackRequest, stream control.PeerControl_StreamAudioServer) error {
	log.Printf("gRPC stream: server handler started session=%q audio_id=%q path=%q", req.SessionId, req.AudioId, req.AudioPath)

	f, err := os.Open(req.AudioPath)
	if err != nil {
		return fmt.Errorf("open audio file: %w", err)
	}
	defer f.Close()

	buf := make([]byte, 32*1024)
	seq := int64(0)
	chunksSent := 0

	for {
		n, err := f.Read(buf)
		if n > 0 {
			chunk := &control.AudioChunk{
				SessionId:   req.SessionId,
				AudioId:     req.AudioId,
				Sequence:    seq,
				Data:        append([]byte(nil), buf[:n]...),
				EndOfStream: false,
			}
			if err := stream.Send(chunk); err != nil {
				log.Printf("gRPC stream: failed to send chunk seq=%d session=%q: %v", seq, req.SessionId, err)
				return err
			}
			chunksSent++
			log.Printf("gRPC stream: sent chunk seq=%d size=%d session=%q", seq, len(chunk.Data), req.SessionId)
			seq++
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("gRPC stream: read error for session=%q: %v", req.SessionId, err)
			return err
		}
	}

	finalChunk := &control.AudioChunk{
		SessionId:   req.SessionId,
		AudioId:     req.AudioId,
		Sequence:    seq,
		EndOfStream: true,
	}
	if err := stream.Send(finalChunk); err != nil {
		log.Printf("gRPC stream: failed to send final chunk session=%q: %v", req.SessionId, err)
		return err
	}

	log.Printf("gRPC stream: finished session=%q chunks_sent=%d", req.SessionId, chunksSent)
	return nil
}

func (s *peerControlServer) receiveAudioFromLeader(ctx context.Context, target, sessionID, audioID, audioPath string, sharedAt time.Time) error {
	log.Printf("gRPC stream: follower opening stream from leader target=%s session=%q audio_id=%q", target, sessionID, audioID)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("gRPC stream: failed to connect to leader target=%s session=%q: %v", target, sessionID, err)
		return err
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	stream, err := client.StreamAudio(ctx, &control.StreamPlaybackRequest{
		SessionId:     sessionID,
		AudioId:       audioID,
		AudioPath:     audioPath,
		SharedAtNanos: sharedAt.UnixNano(),
	})
	if err != nil {
		log.Printf("gRPC stream: failed to start stream from leader target=%s session=%q: %v", target, sessionID, err)
		return err
	}

	var offset time.Duration
	select {
	case currentOffset := <-s.offsetCh:
		offset = currentOffset
	default:
	}

	localPlaybackAt := sync.ConvertSharedTimeToLocal(sharedAt, offset)

	chunksReceived := 0
	tempFile, err := os.CreateTemp("", fmt.Sprintf("silent-%s-%s-*.wav", sanitizeForFilename(sessionID), sanitizeForFilename(audioID)))
	if err != nil {
		log.Printf("gRPC stream: failed to create output file for session=%q: %v", sessionID, err)
		return err
	}
	outputPath := tempFile.Name()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			tempFile.Close()
			log.Printf("gRPC stream: leader stream finished target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(localPlaybackAt, outputPath)
			return nil
		}
		if err != nil {
			tempFile.Close()
			log.Printf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, sessionID, chunksReceived, err)
			return err
		}
		if chunk.EndOfStream {
			tempFile.Close()
			log.Printf("gRPC stream: received final chunk target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(localPlaybackAt, outputPath)
			return nil
		}

		chunksReceived++
		if _, err := tempFile.Write(chunk.Data); err != nil {
			tempFile.Close()
			log.Printf("gRPC stream: failed to write chunk to %s: %v", outputPath, err)
			return err
		}

		log.Printf("gRPC stream: received chunk seq=%d size=%d target=%s session=%q", chunk.Sequence, len(chunk.Data), target, sessionID)
	}
}

func (s *peerControlServer) streamAudioToPeer(ctx context.Context, target, sessionID, audioID, audioPath string, sharedAt time.Time) error {
	log.Printf("gRPC stream: opening client stream to target=%s session=%q audio_id=%q", target, sessionID, audioID)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("gRPC stream: failed to connect to target=%s session=%q: %v", target, sessionID, err)
		return err
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	stream, err := client.StreamAudio(ctx, &control.StreamPlaybackRequest{
		SessionId:     sessionID,
		AudioId:       audioID,
		AudioPath:     audioPath,
		SharedAtNanos: sharedAt.UnixNano(),
	})
	if err != nil {
		log.Printf("gRPC stream: failed to start client stream to target=%s session=%q: %v", target, sessionID, err)
		return err
	}

	chunksReceived := 0
	outputPath := fmt.Sprintf("/tmp/%s-%s.wav", sessionID, audioID)
	f, err := os.Create(outputPath)
	if err != nil {
		log.Printf("gRPC stream: failed to create output file %s: %v", outputPath, err)
		return err
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			f.Close()
			log.Printf("gRPC stream: client stream finished target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(time.Now().Add(100*time.Millisecond), outputPath)
			return nil
		}
		if err != nil {
			f.Close()
			log.Printf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, sessionID, chunksReceived, err)
			return err
		}
		if chunk.EndOfStream {
			f.Close()
			log.Printf("gRPC stream: received final chunk target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(time.Now().Add(100*time.Millisecond), outputPath)
			return nil
		}

		chunksReceived++
		if _, err := f.Write(chunk.Data); err != nil {
			f.Close()
			log.Printf("gRPC stream: failed to write chunk to %s: %v", outputPath, err)
			return err
		}

		log.Printf("gRPC stream: received chunk seq=%d size=%d target=%s session=%q", chunk.Sequence, len(chunk.Data), target, sessionID)
	}
}

func sanitizeForFilename(value string) string {
	var b strings.Builder
	for _, r := range value {
		switch {
		case r >= 'a' && r <= 'z':
			b.WriteRune(r)
		case r >= 'A' && r <= 'Z':
			b.WriteRune(r)
		case r >= '0' && r <= '9':
			b.WriteRune(r)
		case r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	name := strings.Trim(b.String(), "-_.")
	if name == "" {
		return "stream"
	}
	return name
}

func (s *peerControlServer) leaderTarget() string {
	if s.pl != nil {
		if leader := s.pl.Leader(); leader != nil {
			return fmt.Sprintf("%s:%d", hostFromAddress(leader.Address), s.grpcPort)
		}
	}
	return fmt.Sprintf("127.0.0.1:%d", s.grpcPort)
}

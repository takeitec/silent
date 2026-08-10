package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	stdsync "sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"

	"silent/internal/control"
	"silent/internal/models"
	"silent/internal/peerlist"
	syncutil "silent/internal/sync"
)

type peerControlServer struct {
	control.UnimplementedPeerControlServer

	id             string
	isLeader       bool
	pl             *peerlist.PeerList
	grpcPort       int
	wavPath        string
	liveCapture    bool
	captureDevice  string
	offsetCh       chan time.Duration
	sessionMu      stdsync.Mutex
	activeSessions map[string]time.Time
	sessionCancels map[string]context.CancelFunc
	leaderStreams  map[string]*control.StreamPlaybackRequest
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

		target := peerTarget(p.Address, s.grpcPort)
		if target == "" {
			continue
		}
		if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
			log.Printf("gRPC: skipping self-targeted peer %s at %s", p.ID, target)
			continue
		}
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

	localAt := syncutil.ConvertSharedTimeToLocal(sharedAt, offset)

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

func peerTarget(addr string, grpcPort int) string {
	trimmed := strings.TrimSpace(addr)
	if trimmed == "" {
		return ""
	}

	if strings.Contains(trimmed, "@") {
		parts := strings.SplitN(trimmed, "@", 2)
		if len(parts) == 2 && strings.TrimSpace(parts[1]) != "" {
			trimmed = strings.TrimSpace(parts[1])
		}
	}

	if host, port, err := net.SplitHostPort(trimmed); err == nil {
		if port != "" && port != "0" {
			return net.JoinHostPort(host, port)
		}
		return net.JoinHostPort(host, strconv.Itoa(grpcPort))
	}

	host := hostFromAddress(trimmed)
	if host == "" {
		return ""
	}
	return net.JoinHostPort(host, strconv.Itoa(grpcPort))
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
	streamReq := normalizeStreamRequest(req)
	streamFormat := normalizeStreamPlaybackRequest(streamReq)
	sessionID := normalizeSessionID(streamReq.SessionId)
	log.Printf("gRPC stream: StartStreamPlayback session=%q audio_id=%q audio_path=%q shared_at=%s", sessionID, streamReq.AudioId, streamReq.AudioPath, time.Unix(0, streamReq.SharedAtNanos).Format(time.RFC3339Nano))

	if !s.beginSession(sessionID) {
		log.Printf("gRPC stream: ignoring duplicate session=%q", sessionID)
		return &control.StreamPlaybackResponse{
			Accepted:  true,
			SessionId: sessionID,
			Message:   "stream already in progress",
		}, nil
	}

	if !s.isLeader {
		target := s.leaderTarget()
		if target == "" {
			s.finishSession(sessionID)
			log.Printf("gRPC stream: follower has no reachable leader target for session=%q", sessionID)
			return &control.StreamPlaybackResponse{
				Accepted: false,
				Message:  "leader not discovered",
			}, nil
		}
		sharedAt := time.Unix(0, streamReq.SharedAtNanos)

		go func() {
			runCtx, cancel := context.WithCancel(context.Background())
			s.setSessionCancel(sessionID, cancel)
			defer s.clearSessionCancel(sessionID)
			defer s.finishSession(sessionID)
			log.Printf("gRPC stream: follower starting async receive session=%q target=%s at=%s format=%s rate=%d channels=%d", sessionID, target, time.Now().Format(time.RFC3339Nano), streamFormat.SampleFormat, streamFormat.SampleRate, streamFormat.Channels)
			if err := s.receiveAudioFromLeader(runCtx, target, streamReq, sharedAt); err != nil {
				if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
					log.Printf("gRPC stream: follower stream stopped session=%q", sessionID)
					return
				}
				log.Printf("gRPC stream: follower failed to receive session=%q from leader: %v", sessionID, err)
			}
		}()

		return &control.StreamPlaybackResponse{
			Accepted:  true,
			SessionId: sessionID,
			Message:   "stream playback started",
		}, nil
	}

	s.storeLeaderStream(streamReq)

	// Leader path: tell each follower to connect to the leader and receive the stream.
	peers := s.pl.Peers()
	log.Printf("gRPC stream: leader starting stream session=%q for %d follower(s)", streamReq.SessionId, len(peers))
	var kickoffWG stdsync.WaitGroup
	kickoffCount := 0

	for _, p := range peers {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		follower := p
		kickoffCount++
		kickoffWG.Add(1)
		go func() {
			defer kickoffWG.Done()
			s.startStreamOnFollower(context.Background(), follower, streamReq)
		}()
	}

	if kickoffCount == 0 {
		go func(sessionID string) {
			time.Sleep(leaderSessionReleaseCooldown)
			s.finishSession(sessionID)
			log.Printf("gRPC stream: leader released session=%q after cooldown=%s (no followers)", sessionID, leaderSessionReleaseCooldown)
		}(streamReq.SessionId)
	} else {
		go func(sessionID string, fanoutCount int) {
			kickoffWG.Wait()
			time.Sleep(leaderSessionReleaseCooldown)
			s.finishSession(sessionID)
			log.Printf("gRPC stream: leader released session=%q after kickoff completion followers=%d cooldown=%s", sessionID, fanoutCount, leaderSessionReleaseCooldown)
		}(streamReq.SessionId, kickoffCount)
	}

	return &control.StreamPlaybackResponse{
		Accepted:  true,
		SessionId: sessionID,
		Message:   "stream playback started",
	}, nil
}

func (s *peerControlServer) JoinStreamPlayback(ctx context.Context, req *control.JoinStreamRequest) (*control.JoinStreamResponse, error) {
	sessionID := normalizeSessionID(req.GetSessionId())
	followerID := strings.TrimSpace(req.GetFollowerId())
	log.Printf("gRPC stream: JoinStreamPlayback session=%q follower=%q leader=%v", sessionID, followerID, s.isLeader)

	if !s.isLeader {
		target := s.leaderTarget()
		if target == "" {
			return &control.JoinStreamResponse{Accepted: false, SessionId: sessionID, Message: "leader not discovered"}, nil
		}

		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			return nil, fmt.Errorf("connect to leader %s: %w", target, err)
		}
		defer conn.Close()

		client := control.NewPeerControlClient(conn)
		proxyResp, err := client.JoinStreamPlayback(ctx, &control.JoinStreamRequest{
			SessionId:     sessionID,
			FollowerId:    followerID,
			SharedAtNanos: req.GetSharedAtNanos(),
		})
		if err != nil {
			return nil, fmt.Errorf("proxy join stream to leader %s: %w", target, err)
		}
		return proxyResp, nil
	}

	streamReq, ok := s.loadLeaderStream(sessionID)
	if !ok {
		log.Printf("gRPC stream: no active leader stream template for session=%q", sessionID)
		return &control.JoinStreamResponse{Accepted: false, SessionId: sessionID, Message: "session not active on leader"}, nil
	}

	joinAt := req.GetSharedAtNanos()
	if joinAt <= 0 {
		joinAt = time.Now().Add(750 * time.Millisecond).UnixNano()
	}
	streamReq.SharedAtNanos = joinAt

	peers := s.pl.Peers()
	attempted := uint32(0)
	succeeded := uint32(0)

	for _, p := range peers {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}
		if followerID != "" && p.ID != followerID {
			continue
		}
		attempted++
		if s.startStreamOnFollower(ctx, p, streamReq) {
			succeeded++
		}
	}

	if attempted == 0 {
		msg := "no matching follower found"
		if followerID == "" {
			msg = "no followers available"
		}
		return &control.JoinStreamResponse{Accepted: false, SessionId: sessionID, Message: msg}, nil
	}

	accepted := succeeded > 0 && succeeded == attempted
	msg := fmt.Sprintf("late-join attempted=%d succeeded=%d", attempted, succeeded)
	return &control.JoinStreamResponse{Accepted: accepted, SessionId: sessionID, Message: msg, Attempted: attempted, Succeeded: succeeded}, nil
}

func (s *peerControlServer) StopStreamPlayback(ctx context.Context, req *control.StopStreamRequest) (*control.StopStreamResponse, error) {
	sessionID := normalizeSessionID(req.GetSessionId())
	reason := strings.TrimSpace(req.GetReason())
	if reason == "" {
		reason = "stop requested"
	}

	log.Printf("gRPC stream: StopStreamPlayback session=%q reason=%q leader=%v", sessionID, reason, s.isLeader)

	if !s.isLeader {
		log.Printf("gRPC stream: follower received stop for session=%q reason=%q", sessionID, reason)
		stopped := s.cancelSession(sessionID)
		if stopped {
			s.finishSession(sessionID)
			log.Printf("gRPC stream: follower stopped local session=%q", sessionID)
			return &control.StopStreamResponse{Accepted: true, SessionId: sessionID, Message: "session stopped"}, nil
		}
		log.Printf("gRPC stream: follower had no active local session=%q to stop", sessionID)
		return &control.StopStreamResponse{Accepted: false, SessionId: sessionID, Message: "session not active on follower"}, nil
	}

	fanoutCount := 0
	stopErrors := 0
	log.Printf("gRPC stream: leader beginning stop fanout for session=%q", sessionID)
	for _, p := range s.pl.Peers() {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		target := peerTarget(p.Address, s.grpcPort)
		if target == "" {
			log.Printf("gRPC stream: leader stop fanout skipping follower=%s due to empty target", p.ID)
			continue
		}
		if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
			log.Printf("gRPC stream: leader stop fanout skipping self-target follower=%s target=%s", p.ID, target)
			continue
		}

		fanoutCount++
		log.Printf("gRPC stream: leader stop fanout sending follower=%s session=%q target=%s", p.ID, sessionID, target)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			stopErrors++
			log.Printf("gRPC stream: stop fanout connect failed follower=%s target=%s: %v", p.ID, target, err)
			continue
		}

		client := control.NewPeerControlClient(conn)
		rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = client.StopStreamPlayback(rpcCtx, &control.StopStreamRequest{SessionId: sessionID, Reason: reason})
		cancel()
		conn.Close()

		if err != nil {
			stopErrors++
			log.Printf("gRPC stream: stop fanout failed follower=%s target=%s: %v", p.ID, target, err)
			continue
		}

		log.Printf("gRPC stream: leader stop fanout acknowledged follower=%s session=%q", p.ID, sessionID)
	}

	if s.cancelSession(sessionID) {
		s.finishSession(sessionID)
		log.Printf("gRPC stream: leader stopped local session=%q", sessionID)
	} else {
		log.Printf("gRPC stream: leader had no active local session=%q to stop", sessionID)
	}
	s.clearLeaderStream(sessionID)

	msg := fmt.Sprintf("stop broadcast to %d follower(s)", fanoutCount)
	if stopErrors > 0 {
		msg = fmt.Sprintf("%s with %d error(s)", msg, stopErrors)
	}
	log.Printf("gRPC stream: leader completed stop fanout for session=%q followers=%d errors=%d", sessionID, fanoutCount, stopErrors)
	return &control.StopStreamResponse{Accepted: stopErrors == 0, SessionId: sessionID, Message: msg}, nil
}

var sessionLease = 30 * time.Second
var leaderSessionReleaseCooldown = 1500 * time.Millisecond

func (s *peerControlServer) beginSession(sessionID string) bool {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.activeSessions == nil {
		s.activeSessions = make(map[string]time.Time)
	}
	if sessionID == "" {
		sessionID = "default"
	}

	now := time.Now()
	normalized := normalizeSessionID(sessionID)
	if _, active := s.sessionCancels[normalized]; active {
		return false
	}

	if expiry, ok := s.activeSessions[sessionID]; ok {
		if now.Before(expiry) {
			return false
		}
	}

	s.activeSessions[sessionID] = now.Add(sessionLease)
	return true
}

func (s *peerControlServer) finishSession(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	sessionID = normalizeSessionID(sessionID)
	delete(s.activeSessions, sessionID)
}

func normalizeSessionID(sessionID string) string {
	if strings.TrimSpace(sessionID) == "" {
		return "default"
	}
	return strings.TrimSpace(sessionID)
}

func normalizeStreamRequest(req *control.StreamPlaybackRequest) *control.StreamPlaybackRequest {
	if req == nil {
		req = &control.StreamPlaybackRequest{}
	}
	normalized := cloneStreamPlaybackRequest(req)
	normalized.SessionId = normalizeSessionID(normalized.SessionId)
	format := normalizeStreamPlaybackRequest(normalized)
	normalized.SampleRate = uint32(format.SampleRate)
	normalized.Channels = uint32(format.Channels)
	normalized.SampleFormat = format.SampleFormat
	return normalized
}

func cloneStreamPlaybackRequest(req *control.StreamPlaybackRequest) *control.StreamPlaybackRequest {
	if req == nil {
		return &control.StreamPlaybackRequest{}
	}
	return &control.StreamPlaybackRequest{
		SessionId:     req.GetSessionId(),
		AudioId:       req.GetAudioId(),
		AudioPath:     req.GetAudioPath(),
		SharedAtNanos: req.GetSharedAtNanos(),
		Relay:         req.GetRelay(),
		SampleRate:    req.GetSampleRate(),
		Channels:      req.GetChannels(),
		SampleFormat:  req.GetSampleFormat(),
	}
}

func (s *peerControlServer) storeLeaderStream(req *control.StreamPlaybackRequest) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.leaderStreams == nil {
		s.leaderStreams = make(map[string]*control.StreamPlaybackRequest)
	}
	normalized := normalizeSessionID(req.GetSessionId())
	s.leaderStreams[normalized] = cloneStreamPlaybackRequest(req)
	log.Printf("gRPC stream: stored leader stream template session=%q", normalized)
}

func (s *peerControlServer) loadLeaderStream(sessionID string) (*control.StreamPlaybackRequest, bool) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalized := normalizeSessionID(sessionID)
	req, ok := s.leaderStreams[normalized]
	if !ok {
		return nil, false
	}
	return cloneStreamPlaybackRequest(req), true
}

func (s *peerControlServer) clearLeaderStream(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalized := normalizeSessionID(sessionID)
	if s.leaderStreams == nil {
		return
	}
	if _, ok := s.leaderStreams[normalized]; ok {
		delete(s.leaderStreams, normalized)
		log.Printf("gRPC stream: cleared leader stream template session=%q", normalized)
	}
}

func (s *peerControlServer) startStreamOnFollower(ctx context.Context, follower models.Peer, req *control.StreamPlaybackRequest) bool {
	target := peerTarget(follower.Address, s.grpcPort)
	if target == "" {
		log.Printf("gRPC stream: follower=%s has empty target, skipping", follower.ID)
		return false
	}
	if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
		log.Printf("gRPC stream: skipping self-targeted follower=%s at %s", follower.ID, target)
		return false
	}

	kickoffAt := time.Now()
	log.Printf("gRPC stream: leader kickoff follower=%s session=%q target=%s at=%s", follower.ID, req.SessionId, target, kickoffAt.Format(time.RFC3339Nano))

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("gRPC stream: failed to contact follower=%s: %v", follower.ID, err)
		return false
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	rpcCtx := ctx
	if rpcCtx == nil {
		rpcCtx = context.Background()
	}
	timeoutCtx, cancel := context.WithTimeout(rpcCtx, 5*time.Second)
	defer cancel()

	if _, err := client.StartStreamPlayback(timeoutCtx, req); err != nil {
		log.Printf("gRPC stream: follower=%s rejected stream start: %v", follower.ID, err)
		return false
	}

	log.Printf("gRPC stream: follower=%s accepted stream start session=%q target=%s ack_after=%s", follower.ID, req.SessionId, target, time.Since(kickoffAt))
	return true
}

func (s *peerControlServer) setSessionCancel(sessionID string, cancel context.CancelFunc) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if s.sessionCancels == nil {
		s.sessionCancels = make(map[string]context.CancelFunc)
	}
	normalized := normalizeSessionID(sessionID)
	s.sessionCancels[normalized] = cancel
	log.Printf("gRPC stream: registered cancellable session=%q", normalized)
}

func (s *peerControlServer) clearSessionCancel(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	normalized := normalizeSessionID(sessionID)
	if _, ok := s.sessionCancels[normalized]; ok {
		delete(s.sessionCancels, normalized)
		log.Printf("gRPC stream: cleared cancellable session=%q", normalized)
	}
}

func (s *peerControlServer) cancelSession(sessionID string) bool {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalized := normalizeSessionID(sessionID)
	cancel, ok := s.sessionCancels[normalized]
	if !ok {
		log.Printf("gRPC stream: cancel requested for non-active session=%q", normalized)
		return false
	}
	delete(s.sessionCancels, normalized)
	log.Printf("gRPC stream: canceling active session=%q", normalized)
	cancel()
	return true
}

func streamTarget(ctx context.Context) string {
	if ctx == nil {
		return "<unknown>"
	}

	if p, ok := peer.FromContext(ctx); ok {
		if p.Addr != nil {
			return p.Addr.String()
		}
	}

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get("x-peer-address"); len(vals) > 0 {
			return vals[0]
		}
		if vals := md.Get("peer"); len(vals) > 0 {
			return vals[0]
		}
	}

	if v := ctx.Value("peer"); v != nil {
		if s, ok := v.(string); ok {
			return s
		}
	}

	return "<unknown>"
}

func (s *peerControlServer) StreamAudio(req *control.StreamPlaybackRequest, stream control.PeerControl_StreamAudioServer) error {
	target := streamTarget(stream.Context())
	streamFormat := normalizeStreamPlaybackRequest(req)
	log.Printf("gRPC stream: server handler started session=%q audio_id=%q path=%q target=%s", req.SessionId, req.AudioId, req.AudioPath, target)

	source, sourceName, closeSource, err := s.openStreamSource(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := closeSource(); closeErr != nil {
			log.Printf("gRPC stream: source close error (%s): %v", sourceName, closeErr)
		}
	}()

	log.Printf("gRPC stream: using source=%s for session=%q", sourceName, req.SessionId)

	buf := make([]byte, streamFormat.ChunkBytes)
	seq := int64(0)
	chunksSent := 0

	for {
		n, err := source.Read(buf)
		if n > 0 {
			chunk := &control.AudioChunk{
				SessionId:   req.SessionId,
				AudioId:     req.AudioId,
				Sequence:    seq,
				Data:        append([]byte(nil), buf[:n]...),
				EndOfStream: false,
			}
			if err := stream.Send(chunk); err != nil {
				log.Printf("gRPC stream: failed to send chunk seq=%d session=%q target=%s: %v", seq, req.SessionId, target, err)
				return err
			}
			chunksSent++
			log.Printf("gRPC stream: sent chunk seq=%d size=%d session=%q target=%s", seq, len(chunk.Data), req.SessionId, target)
			seq++
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("gRPC stream: read error from source=%s for session=%q: %v", sourceName, req.SessionId, err)
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
		log.Printf("gRPC stream: failed to send final chunk session=%q target=%s: %v", req.SessionId, target, err)
		return err
	}

	log.Printf("gRPC stream: finished session=%q chunks_sent=%d target=%s", req.SessionId, chunksSent, target)
	return nil
}

func (s *peerControlServer) receiveAudioFromLeader(ctx context.Context, target string, req *control.StreamPlaybackRequest, sharedAt time.Time) error {
	streamFormat := normalizeStreamPlaybackRequest(req)
	log.Printf("gRPC stream: follower opening stream from leader target=%s session=%q audio_id=%q", target, req.SessionId, req.AudioId)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("gRPC stream: failed to connect to leader target=%s session=%q: %v", target, req.SessionId, err)
		return err
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	stream, err := client.StreamAudio(ctx, req)
	if err != nil {
		log.Printf("gRPC stream: failed to start stream from leader target=%s session=%q: %v", target, req.SessionId, err)
		return err
	}

	var offset time.Duration
	select {
	case currentOffset := <-s.offsetCh:
		offset = currentOffset
	default:
	}

	localPlaybackAt := syncutil.ConvertSharedTimeToLocal(sharedAt, offset)
	firstChunkLogged := false

	chunksReceived := 0
	liveSink := io.WriteCloser(nil)
	closeLiveSink := func() error { return nil }
	liveLogPath := ""
	lastLiveErrorLogPath := ""
	restartAttempted := false
	terminateOnLiveFailure := true
	startLiveSink := func(reason string) bool {
		sink, closeFn, logPath, err := startStreamingPlaybackWithFormatAndLog(streamFormat, req.SessionId)
		if err != nil {
			if reason == "initial" {
				log.Printf("gRPC stream: live playback unavailable for session=%q, will use temp file playback: %v", req.SessionId, err)
			} else {
				log.Printf("gRPC stream: live playback restart failed for session=%q after %s: %v", req.SessionId, reason, err)
			}
			return false
		}
		liveSink = sink
		closeLiveSink = closeFn
		liveLogPath = logPath
		log.Printf("gRPC stream: live playback process started session=%q log=%s", req.SessionId, liveLogPath)
		return true
	}
	_ = startLiveSink("initial")
	liveSinkDisabled := false
	disableLiveSink := func(reason string, cause error) {
		if liveSink == nil || liveSinkDisabled {
			return
		}
		currentClose := closeLiveSink
		currentLogPath := liveLogPath
		lastLiveErrorLogPath = currentLogPath
		liveSinkDisabled = true
		logTail := ffplayLogTail(currentLogPath)
		if currentLogPath != "" {
			log.Printf("gRPC stream: disabling live playback for session=%q (%s): %v (ffplay log: %s)", req.SessionId, reason, cause, currentLogPath)
		} else {
			log.Printf("gRPC stream: disabling live playback for session=%q (%s): %v", req.SessionId, reason, cause)
		}
		if logTail != "" {
			log.Printf("gRPC stream: ffplay stderr tail for session=%q: %s", req.SessionId, logTail)
		}
		_ = liveSink.Close()
		liveSink = nil
		closeLiveSink = func() error { return nil }
		liveLogPath = ""
		go func() {
			if err := currentClose(); err != nil {
				if currentLogPath != "" {
					log.Printf("gRPC stream: live playback process ended with error for session=%q: %v (ffplay log: %s)", req.SessionId, err, currentLogPath)
				} else {
					log.Printf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
				}
			}
		}()
	}
	attemptRestart := func(trigger string) {
		if restartAttempted {
			return
		}
		restartAttempted = true
		if startLiveSink(trigger) {
			liveSinkDisabled = false
			log.Printf("gRPC stream: live playback restart succeeded for session=%q after %s", req.SessionId, trigger)
		}
	}
	failDueToLivePlayback := func(trigger string) error {
		if !terminateOnLiveFailure {
			return nil
		}
		msg := fmt.Sprintf("gRPC stream: live playback unavailable after retry for session=%q (trigger=%s)", req.SessionId, trigger)
		if lastLiveErrorLogPath != "" {
			msg = fmt.Sprintf("%s (ffplay log: %s)", msg, lastLiveErrorLogPath)
		}
		log.Printf("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	var liveBuffer bytes.Buffer
	liveStarted := false

	tempFile, err := os.CreateTemp("", fmt.Sprintf("silent-%s-%s-*.pcm", sanitizeForFilename(req.SessionId), sanitizeForFilename(req.AudioId)))
	if err != nil {
		log.Printf("gRPC stream: failed to create output file for session=%q: %v", req.SessionId, err)
		return err
	}
	outputPath := tempFile.Name()

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			tempFile.Close()
			log.Printf("gRPC stream: leader stream finished target=%s session=%q chunks_received=%d output=%s", target, req.SessionId, chunksReceived, outputPath)
			if liveSink != nil {
				if !liveStarted {
					delay := time.Until(localPlaybackAt)
					if delay > 0 {
						time.Sleep(delay)
					}
					if liveBuffer.Len() > 0 {
						if _, writeErr := liveSink.Write(liveBuffer.Bytes()); writeErr != nil {
							disableLiveSink("buffer flush on EOF", writeErr)
						}
					}
				}
				if liveSink != nil {
					go func() {
						if err := closeLiveSink(); err != nil {
							log.Printf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
						}
					}()
				}
			} else {
				go scheduleRawPlayback(localPlaybackAt, outputPath, streamFormat)
			}
			return nil
		}
		if err != nil {
			tempFile.Close()
			if liveSink != nil {
				disableLiveSink("stream receive error", err)
			}
			if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
				log.Printf("gRPC stream: receive loop canceled target=%s session=%q chunks_received=%d", target, req.SessionId, chunksReceived)
				return err
			}
			log.Printf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, req.SessionId, chunksReceived, err)
			return err
		}
		if chunk.EndOfStream {
			tempFile.Close()
			log.Printf("gRPC stream: received final chunk target=%s session=%q chunks_received=%d output=%s", target, req.SessionId, chunksReceived, outputPath)
			if liveSink != nil {
				if !liveStarted {
					delay := time.Until(localPlaybackAt)
					if delay > 0 {
						time.Sleep(delay)
					}
					if liveBuffer.Len() > 0 {
						if _, writeErr := liveSink.Write(liveBuffer.Bytes()); writeErr != nil {
							disableLiveSink("buffer flush on end-of-stream", writeErr)
						}
					}
				}
				if liveSink != nil {
					go func() {
						if err := closeLiveSink(); err != nil {
							log.Printf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
						}
					}()
				}
			} else {
				go scheduleRawPlayback(localPlaybackAt, outputPath, streamFormat)
			}
			return nil
		}

		chunksReceived++
		if !firstChunkLogged {
			firstChunkLogged = true
			now := time.Now()
			log.Printf("gRPC stream: follower first chunk session=%q target=%s at=%s until_local_playback=%s", req.SessionId, target, now.Format(time.RFC3339Nano), time.Until(localPlaybackAt))
		}
		if _, err := tempFile.Write(chunk.Data); err != nil {
			tempFile.Close()
			if liveSink != nil {
				disableLiveSink("temp file write error", err)
			}
			log.Printf("gRPC stream: failed to write chunk to %s: %v", outputPath, err)
			return err
		}

		if liveSink == nil && !restartAttempted {
			attemptRestart("live sink unavailable before chunk write")
			if liveSink == nil {
				tempFile.Close()
				if err := failDueToLivePlayback("startup failure"); err != nil {
					return err
				}
			}
		}

		if liveSink != nil {
			now := time.Now()
			if !liveStarted && now.Before(localPlaybackAt) {
				if _, err := liveBuffer.Write(chunk.Data); err != nil {
					log.Printf("gRPC stream: failed to buffer live chunk session=%q: %v", req.SessionId, err)
				}
			} else {
				if !liveStarted {
					if liveBuffer.Len() > 0 {
						if _, err := liveSink.Write(liveBuffer.Bytes()); err != nil {
							disableLiveSink("buffer flush", err)
							attemptRestart("buffer flush")
						}
					}
					if liveSink != nil {
						liveStarted = true
					}
				}
				if liveSink != nil {
					if _, err := liveSink.Write(chunk.Data); err != nil {
						disableLiveSink(fmt.Sprintf("live chunk write seq=%d", chunk.Sequence), err)
						attemptRestart(fmt.Sprintf("live chunk write seq=%d", chunk.Sequence))
						if liveSink == nil {
							tempFile.Close()
							if err := failDueToLivePlayback(fmt.Sprintf("chunk write seq=%d", chunk.Sequence)); err != nil {
								return err
							}
						}
					}
				}
			}
		}

		log.Printf("gRPC stream: received chunk seq=%d size=%d target=%s session=%q", chunk.Sequence, len(chunk.Data), target, req.SessionId)
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

func ffplayLogTail(logPath string) string {
	if strings.TrimSpace(logPath) == "" {
		return ""
	}

	data, err := os.ReadFile(logPath)
	if err != nil || len(data) == 0 {
		return ""
	}

	const maxTail = 512
	if len(data) > maxTail {
		data = data[len(data)-maxTail:]
	}

	tail := strings.TrimSpace(string(data))
	if tail == "" {
		return ""
	}
	return strings.ReplaceAll(tail, "\n", " | ")
}

func (s *peerControlServer) leaderTarget() string {
	if s.pl != nil {
		if leader := s.pl.Leader(); leader != nil {
			if leader.ID == s.id {
				return ""
			}
			if target := peerTarget(leader.Address, s.grpcPort); target != "" {
				if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
					return ""
				}
				return target
			}
		}
	}
	return ""
}

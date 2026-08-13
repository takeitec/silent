package main

import (
	"context"
	"errors"
	"fmt"
	"io"
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

	id                   string
	isLeader             bool
	pl                   *peerlist.PeerList
	grpcPort             int
	wavPath              string
	liveCapture          bool
	captureDevice        string
	streamJitter         time.Duration
	streamJitterAdaptive bool
	streamJitterMin      time.Duration
	streamJitterMax      time.Duration
	streamJitterStep     time.Duration
	chunkLogStdoutMode   string
	chunkLogFileMode     string
	chunkLogEvery        int
	chunkLogFilePath     string
	chunkLogFile         *os.File
	chunkLogMu           stdsync.Mutex
	offsetCh             chan time.Duration
	sessionMu            stdsync.Mutex
	activeSessions       map[string]time.Time
	sessionCancels       map[string]context.CancelFunc
	leaderStreams        map[string]*control.StreamPlaybackRequest
}

const (
	chunkLogModeOff       = "off"
	chunkLogModeMilestone = "milestone"
	chunkLogModeAll       = "all"
)

func normalizeChunkLogMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case chunkLogModeOff, chunkLogModeMilestone, chunkLogModeAll:
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return chunkLogModeMilestone
	}
}

func (s *peerControlServer) shouldLogChunk(mode string, seq int64, size, expectedSize int) bool {
	mode = normalizeChunkLogMode(mode)
	if mode == chunkLogModeOff {
		return false
	}
	if mode == chunkLogModeAll {
		return true
	}

	every := s.chunkLogEvery
	if every <= 0 {
		every = 50
	}
	if seq%int64(every) == 0 {
		return true
	}
	if expectedSize > 0 && size != expectedSize {
		return true
	}
	return false
}

func (s *peerControlServer) logChunkEvent(direction, sessionID, target string, seq int64, size, expectedSize int, extra string) {
	if !s.shouldLogChunk(s.chunkLogStdoutMode, seq, size, expectedSize) && !s.shouldLogChunk(s.chunkLogFileMode, seq, size, expectedSize) {
		return
	}

	msg := fmt.Sprintf("gRPC stream: %s chunk seq=%d size=%d session=%q target=%s", direction, seq, size, sessionID, target)
	if expectedSize > 0 && size != expectedSize {
		msg = fmt.Sprintf("%s expected_size=%d", msg, expectedSize)
	}
	if strings.TrimSpace(extra) != "" {
		msg = fmt.Sprintf("%s %s", msg, strings.TrimSpace(extra))
	}

	if s.shouldLogChunk(s.chunkLogStdoutMode, seq, size, expectedSize) {
		logInfof("%s", msg)
	}

	if s.shouldLogChunk(s.chunkLogFileMode, seq, size, expectedSize) && s.chunkLogFile != nil {
		s.chunkLogMu.Lock()
		_, _ = fmt.Fprintf(s.chunkLogFile, "%s %s\n", time.Now().Format(time.RFC3339Nano), msg)
		s.chunkLogMu.Unlock()
	}
}

func (s *peerControlServer) StartPlayback(ctx context.Context, req *control.PlaybackRequest) (*control.PlaybackResponse, error) {
	logInfof("gRPC: StartPlayback received audio_id=%q audio_path=%q", req.AudioId, req.AudioPath)

	if !s.isLeader {
		logWarnf("gRPC: StartPlayback rejected because this peer is not the leader")
		return &control.PlaybackResponse{Accepted: false, Message: "not leader"}, nil
	}

	sharedAt := time.Now().Add(3 * time.Second)
	peers := s.pl.Peers()

	logInfof("gRPC: leader will notify %d follower(s) at shared time %s", len(peers), sharedAt.Format(time.RFC3339Nano))

	for _, p := range peers {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		target := peerTarget(p.Address, s.grpcPort)
		if target == "" {
			continue
		}
		if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
			logInfof("gRPC: skipping self-targeted peer %s at %s", p.ID, target)
			continue
		}
		logInfof("gRPC: notifying peer %s at %s", p.ID, target)

		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			logWarnf("gRPC: notify %s failed to connect: %v", p.ID, err)
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
			logWarnf("gRPC: notify playback to %s failed: %v", p.ID, err)
			continue
		}

		logInfof("gRPC: notify playback to %s succeeded", p.ID)
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

	logInfof("received playback command for %s at local %s", req.AudioId, localAt.Format(time.RFC3339Nano))

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

	logInfof("gRPC server listening on %s", addr)

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
	logInfof("gRPC stream: StartStreamPlayback session=%q audio_id=%q audio_path=%q shared_at=%s", sessionID, streamReq.AudioId, streamReq.AudioPath, time.Unix(0, streamReq.SharedAtNanos).Format(time.RFC3339Nano))

	// If this peer is already handling the same session, reject the duplicate request.
	if !s.beginSession(sessionID) {
		logInfof("gRPC stream: ignoring duplicate session=%q", sessionID)
		return &control.StreamPlaybackResponse{
			Accepted:  true,
			SessionId: sessionID,
			Message:   "stream already in progress",
		}, nil
	}

	// Follower path: if this peer is not the leader, it should connect to the leader and receive the stream.
	if !s.isLeader {
		target := s.leaderTarget()
		if target == "" {
			s.finishSession(sessionID)
			logInfof("gRPC stream: follower has no reachable leader target for session=%q", sessionID)
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
			logInfof("gRPC stream: follower starting async receive session=%q target=%s at=%s format=%s rate=%d channels=%d", sessionID, target, time.Now().Format(time.RFC3339Nano), streamFormat.SampleFormat, streamFormat.SampleRate, streamFormat.Channels)
			if err := s.receiveAudioFromLeader(runCtx, target, streamReq, sharedAt); err != nil {
				if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
					logInfof("gRPC stream: follower stream stopped session=%q", sessionID)
					return
				}
				logWarnf("gRPC stream: follower failed to receive session=%q from leader: %v", sessionID, err)
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
	logInfof("gRPC stream: leader starting stream session=%q for %d follower(s)", streamReq.SessionId, len(peers))
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
			logInfof("gRPC stream: leader released session=%q after cooldown=%s (no followers)", sessionID, leaderSessionReleaseCooldown)
		}(streamReq.SessionId)
	} else {
		go func(sessionID string, fanoutCount int) {
			kickoffWG.Wait()
			time.Sleep(leaderSessionReleaseCooldown)
			s.finishSession(sessionID)
			logInfof("gRPC stream: leader released session=%q after kickoff completion followers=%d cooldown=%s", sessionID, fanoutCount, leaderSessionReleaseCooldown)
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
	logInfof("gRPC stream: JoinStreamPlayback session=%q follower=%q leader=%v", sessionID, followerID, s.isLeader)

	// Follower path: if this peer is not the leader, it should connect to the leader and request to join the stream.
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

	// Leader path: if this peer is the leader, it should check if the session is active and then fan out the join request to followers.
	streamReq, ok := s.loadLeaderStream(sessionID)
	if !ok {
		logInfof("gRPC stream: no active leader stream template for session=%q", sessionID)
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

	logInfof("gRPC stream: StopStreamPlayback session=%q reason=%q leader=%v", sessionID, reason, s.isLeader)

	// Follower path: if this peer is not the leader, it should stop its local session if active.
	if !s.isLeader {
		logInfof("gRPC stream: follower received stop for session=%q reason=%q", sessionID, reason)
		stopped := s.cancelSession(sessionID)
		if stopped {
			s.finishSession(sessionID)
			logInfof("gRPC stream: follower stopped local session=%q", sessionID)
			return &control.StopStreamResponse{Accepted: true, SessionId: sessionID, Message: "session stopped"}, nil
		}
		logInfof("gRPC stream: follower had no active local session=%q to stop", sessionID)
		return &control.StopStreamResponse{Accepted: false, SessionId: sessionID, Message: "session not active on follower"}, nil
	}

	// Leader path: if this peer is the leader, it should fan out the stop request to all followers and stop its own session.
	fanoutCount := 0
	stopErrors := 0
	logInfof("gRPC stream: leader beginning stop fanout for session=%q", sessionID)
	for _, p := range s.pl.Peers() {
		if p.ID == s.id || p.Role == models.RoleLeader {
			continue
		}

		target := peerTarget(p.Address, s.grpcPort)
		if target == "" {
			logInfof("gRPC stream: leader stop fanout skipping follower=%s due to empty target", p.ID)
			continue
		}
		if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
			logInfof("gRPC stream: leader stop fanout skipping self-target follower=%s target=%s", p.ID, target)
			continue
		}

		fanoutCount++
		logInfof("gRPC stream: leader stop fanout sending follower=%s session=%q target=%s", p.ID, sessionID, target)
		conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
		if err != nil {
			stopErrors++
			logWarnf("gRPC stream: stop fanout connect failed follower=%s target=%s: %v", p.ID, target, err)
			continue
		}

		client := control.NewPeerControlClient(conn)
		rpcCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		_, err = client.StopStreamPlayback(rpcCtx, &control.StopStreamRequest{SessionId: sessionID, Reason: reason})
		cancel()
		conn.Close()

		if err != nil {
			stopErrors++
			logWarnf("gRPC stream: stop fanout failed follower=%s target=%s: %v", p.ID, target, err)
			continue
		}

		logInfof("gRPC stream: leader stop fanout acknowledged follower=%s session=%q", p.ID, sessionID)
	}

	if s.cancelSession(sessionID) {
		s.finishSession(sessionID)
		logInfof("gRPC stream: leader stopped local session=%q", sessionID)
	} else {
		logInfof("gRPC stream: leader had no active local session=%q to stop", sessionID)
	}
	s.clearLeaderStream(sessionID)

	msg := fmt.Sprintf("stop broadcast to %d follower(s)", fanoutCount)
	if stopErrors > 0 {
		msg = fmt.Sprintf("%s with %d error(s)", msg, stopErrors)
	}
	logWarnf("gRPC stream: leader completed stop fanout for session=%q followers=%d errors=%d", sessionID, fanoutCount, stopErrors)
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
	logInfof("gRPC stream: stored leader stream template session=%q", normalized)
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
		logInfof("gRPC stream: cleared leader stream template session=%q", normalized)
	}
}

func (s *peerControlServer) startStreamOnFollower(ctx context.Context, follower models.Peer, req *control.StreamPlaybackRequest) bool {
	target := peerTarget(follower.Address, s.grpcPort)
	if target == "" {
		logInfof("gRPC stream: follower=%s has empty target, skipping", follower.ID)
		return false
	}
	if target == fmt.Sprintf("127.0.0.1:%d", s.grpcPort) || target == fmt.Sprintf("localhost:%d", s.grpcPort) {
		logInfof("gRPC stream: skipping self-targeted follower=%s at %s", follower.ID, target)
		return false
	}

	kickoffAt := time.Now()
	logInfof("gRPC stream: leader kickoff follower=%s session=%q target=%s at=%s", follower.ID, req.SessionId, target, kickoffAt.Format(time.RFC3339Nano))

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logWarnf("gRPC stream: failed to contact follower=%s: %v", follower.ID, err)
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
		logWarnf("gRPC stream: follower=%s rejected stream start: %v", follower.ID, err)
		return false
	}

	logInfof("gRPC stream: follower=%s accepted stream start session=%q target=%s ack_after=%s", follower.ID, req.SessionId, target, time.Since(kickoffAt))
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
	logInfof("gRPC stream: registered cancellable session=%q", normalized)
}

func (s *peerControlServer) clearSessionCancel(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	normalized := normalizeSessionID(sessionID)
	if _, ok := s.sessionCancels[normalized]; ok {
		delete(s.sessionCancels, normalized)
		logInfof("gRPC stream: cleared cancellable session=%q", normalized)
	}
}

func (s *peerControlServer) cancelSession(sessionID string) bool {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalized := normalizeSessionID(sessionID)
	cancel, ok := s.sessionCancels[normalized]
	if !ok {
		logInfof("gRPC stream: cancel requested for non-active session=%q", normalized)
		return false
	}
	delete(s.sessionCancels, normalized)
	logInfof("gRPC stream: canceling active session=%q", normalized)
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
	logInfof("gRPC stream: server handler started session=%q audio_id=%q path=%q target=%s", req.SessionId, req.AudioId, req.AudioPath, target)

	source, sourceName, closeSource, err := s.openStreamSource(req)
	if err != nil {
		return err
	}
	defer func() {
		if closeErr := closeSource(); closeErr != nil {
			logWarnf("gRPC stream: source close error (%s): %v", sourceName, closeErr)
		}
	}()

	logInfof("gRPC stream: using source=%s for session=%q", sourceName, req.SessionId)

	buf := make([]byte, streamFormat.ChunkBytes)
	chunkDur := streamChunkDuration(streamFormat)
	paceStream := sourceName != "live-capture" && chunkDur > 0
	nextSendAt := time.Time{}
	seq := int64(0)
	chunksSent := 0

	for {
		n, err := io.ReadFull(source, buf)
		if n > 0 {
			if paceStream {
				now := time.Now()
				if nextSendAt.IsZero() {
					nextSendAt = now
				}
				if wait := time.Until(nextSendAt); wait > 0 {
					timer := time.NewTimer(wait)
					select {
					case <-stream.Context().Done():
						if !timer.Stop() {
							<-timer.C
						}
						return stream.Context().Err()
					case <-timer.C:
					}
				}
			}

			chunk := &control.AudioChunk{
				SessionId:   req.SessionId,
				AudioId:     req.AudioId,
				Sequence:    seq,
				Data:        append([]byte(nil), buf[:n]...),
				SentAtNanos: time.Now().UnixNano(),
				EndOfStream: false,
			}
			if err := stream.Send(chunk); err != nil {
				logWarnf("gRPC stream: failed to send chunk seq=%d session=%q target=%s: %v", seq, req.SessionId, target, err)
				return err
			}
			chunksSent++
			s.logChunkEvent("sent", req.SessionId, target, seq, len(chunk.Data), streamFormat.ChunkBytes, "")
			if paceStream {
				nextSendAt = nextSendAt.Add(chunkDur)
			}
			seq++
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			break
		}
		if err != nil {
			logWarnf("gRPC stream: read error from source=%s for session=%q: %v", sourceName, req.SessionId, err)
			return err
		}
	}

	finalChunk := &control.AudioChunk{
		SessionId:   req.SessionId,
		AudioId:     req.AudioId,
		Sequence:    seq,
		SentAtNanos: time.Now().UnixNano(),
		EndOfStream: true,
	}
	if err := stream.Send(finalChunk); err != nil {
		logWarnf("gRPC stream: failed to send final chunk session=%q target=%s: %v", req.SessionId, target, err)
		return err
	}

	logInfof("gRPC stream: finished session=%q chunks_sent=%d target=%s", req.SessionId, chunksSent, target)
	return nil
}

func (s *peerControlServer) receiveAudioFromLeader(ctx context.Context, target string, req *control.StreamPlaybackRequest, sharedAt time.Time) error {
	streamFormat := normalizeStreamPlaybackRequest(req)
	logInfof("gRPC stream: follower opening stream from leader target=%s session=%q audio_id=%q", target, req.SessionId, req.AudioId)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logWarnf("gRPC stream: failed to connect to leader target=%s session=%q: %v", target, req.SessionId, err)
		return err
	}
	defer conn.Close()

	client := control.NewPeerControlClient(conn)
	stream, err := client.StreamAudio(ctx, req)
	if err != nil {
		logWarnf("gRPC stream: failed to start stream from leader target=%s session=%q: %v", target, req.SessionId, err)
		return err
	}

	var offset time.Duration
	select {
	case currentOffset := <-s.offsetCh:
		offset = currentOffset
	default:
	}

	localPlaybackAt := syncutil.ConvertSharedTimeToLocal(sharedAt, offset)
	playoutDelay := s.streamJitter
	if playoutDelay <= 0 {
		playoutDelay = 200 * time.Millisecond
	}
	adaptiveEnabled := s.streamJitterAdaptive
	adaptiveMin := s.streamJitterMin
	adaptiveMax := s.streamJitterMax
	adaptiveStep := s.streamJitterStep
	if adaptiveMin <= 0 {
		adaptiveMin = 80 * time.Millisecond
	}
	if adaptiveMax < adaptiveMin {
		adaptiveMax = adaptiveMin
	}
	if adaptiveStep <= 0 {
		adaptiveStep = 20 * time.Millisecond
	}
	if playoutDelay < adaptiveMin {
		playoutDelay = adaptiveMin
	}
	if playoutDelay > adaptiveMax {
		playoutDelay = adaptiveMax
	}
	chunkDur := streamChunkDuration(streamFormat)
	silenceChunk := make([]byte, streamFormat.ChunkBytes)

	chunksReceived := 0
	firstChunkLogged := false
	metrics := newStreamHealthMetrics(playoutDelay)

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
				logWarnf("gRPC stream: live playback unavailable for session=%q, will use temp file playback: %v", req.SessionId, err)
			} else {
				logWarnf("gRPC stream: live playback restart failed for session=%q after %s: %v", req.SessionId, reason, err)
			}
			return false
		}
		liveSink = sink
		closeLiveSink = closeFn
		liveLogPath = logPath
		logInfof("gRPC stream: live playback process started session=%q log=%s", req.SessionId, liveLogPath)
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
			logInfof("gRPC stream: disabling live playback for session=%q (%s): %v (ffplay log: %s)", req.SessionId, reason, cause, currentLogPath)
		} else {
			logInfof("gRPC stream: disabling live playback for session=%q (%s): %v", req.SessionId, reason, cause)
		}
		if logTail != "" {
			logInfof("gRPC stream: ffplay stderr tail for session=%q: %s", req.SessionId, logTail)
		}
		_ = liveSink.Close()
		liveSink = nil
		closeLiveSink = func() error { return nil }
		liveLogPath = ""
		go func() {
			if err := currentClose(); err != nil {
				if currentLogPath != "" {
					logWarnf("gRPC stream: live playback process ended with error for session=%q: %v (ffplay log: %s)", req.SessionId, err, currentLogPath)
				} else {
					logWarnf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
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
			logInfof("gRPC stream: live playback restart succeeded for session=%q after %s", req.SessionId, trigger)
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
		logInfof("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	tempFile, err := os.CreateTemp("", fmt.Sprintf("silent-%s-%s-*.pcm", sanitizeForFilename(req.SessionId), sanitizeForFilename(req.AudioId)))
	if err != nil {
		logErrorf("gRPC stream: failed to create output file for session=%q: %v", req.SessionId, err)
		return err
	}
	outputPath := tempFile.Name()
	defer tempFile.Close()

	writeAudio := func(payload []byte, seq int64, source string) error {
		if len(payload) == 0 {
			return nil
		}
		if _, err := tempFile.Write(payload); err != nil {
			if liveSink != nil {
				disableLiveSink("temp file write error", err)
			}
			return fmt.Errorf("write chunk (%s) seq=%d to %s: %w", source, seq, outputPath, err)
		}

		if liveSink == nil && !restartAttempted {
			attemptRestart("live sink unavailable before playout")
			if liveSink == nil {
				if err := failDueToLivePlayback("startup failure"); err != nil {
					return err
				}
			}
		}

		if liveSink != nil {
			if _, err := liveSink.Write(payload); err != nil {
				disableLiveSink(fmt.Sprintf("live write (%s) seq=%d", source, seq), err)
				attemptRestart(fmt.Sprintf("live write (%s) seq=%d", source, seq))
				if liveSink == nil {
					if err := failDueToLivePlayback(fmt.Sprintf("live write (%s) seq=%d", source, seq)); err != nil {
						return err
					}
				}
			}
		}

		return nil
	}

	type recvEnvelope struct {
		chunk *control.AudioChunk
		err   error
		at    time.Time
	}

	recvCh := make(chan recvEnvelope, 32)
	go func() {
		defer close(recvCh)
		for {
			chunk, recvErr := stream.Recv()
			recvCh <- recvEnvelope{chunk: chunk, err: recvErr, at: time.Now()}
			if recvErr != nil {
				return
			}
			if chunk.GetEndOfStream() {
				return
			}
		}
	}()

	pending := make(map[int64][]byte)
	initialized := false
	playoutStarted := false
	requestedStart := !sharedAt.IsZero() && req.GetSharedAtNanos() > 0
	startAt := time.Time{}
	playoutStartedAt := time.Time{}
	nextPlayoutAt := time.Time{}
	expectedSeq := int64(0)
	endSeq := int64(-1)
	eosSeen := false
	lastChunkAt := time.Time{}
	lastStallLogAt := time.Time{}
	lastHealthLogAt := time.Now()
	lastAdaptiveTuneAt := time.Now()
	lastAdaptiveReceived := int64(0)
	lastAdaptiveLate := int64(0)
	lastAdaptiveUnderflows := int64(0)
	lastAdaptiveCatchup := int64(0)
	stableAdaptiveWindows := 0
	lastDelayError := time.Duration(0)
	hardResyncWindowUntil := time.Time{}
	lastHardResyncAt := time.Time{}
	latestSeqReceived := int64(-1)
	missingSeqHoldCount := 0

	const maxPlayoutStepsPerDrain = 8
	const receiveStallLogInterval = 2 * time.Second
	const healthLogInterval = 5 * time.Second
	const adaptiveTuneInterval = 5 * time.Second
	const adaptiveDecreaseStableWindows = 2
	const softResyncBand = 120 * time.Millisecond
	const softResyncMinStep = 10 * time.Millisecond
	const softResyncMaxStep = 18 * time.Millisecond
	const softResyncGain = 0.10
	const softResyncCooldown = 800 * time.Millisecond
	const hardResyncDelayThreshold = 1200 * time.Millisecond
	const hardResyncChunkThreshold = 90
	const hardResyncCooldown = 10 * time.Second
	const hardResyncWindow = 6 * time.Second
	const hardResyncRetainChunks = 3
	const ingressHardResyncScale = 4
	const missingSeqHoldChunks = 6
	const resyncWarmup = 3 * time.Second
	const minSoftResyncBufferedChunks = 4

	lastSoftResyncAt := time.Time{}

	currentQueueDelay := func() time.Duration {
		if chunkDur <= 0 {
			return 0
		}
		return time.Duration(len(pending)) * chunkDur
	}

	absoluteDuration := func(v time.Duration) time.Duration {
		if v < 0 {
			return -v
		}
		return v
	}

	hardResyncAllowed := func(now time.Time) bool {
		if lastHardResyncAt.IsZero() {
			return true
		}
		return now.Sub(lastHardResyncAt) >= hardResyncCooldown
	}

	hardResync := func(now time.Time, reason string) {
		if !initialized || !playoutStarted {
			return
		}

		oldExpected := expectedSeq
		oldPending := len(pending)
		oldQueueDelay := currentQueueDelay()

		anchorSeq := expectedSeq
		if oldPending > hardResyncRetainChunks {
			anchorSeq = latestSeqReceived
			if anchorSeq < expectedSeq {
				anchorSeq = expectedSeq
			}
			anchorSeq = anchorSeq - int64(hardResyncRetainChunks-1)
			if anchorSeq < expectedSeq {
				anchorSeq = expectedSeq
			}
		}

		dropped := 0
		for seq := range pending {
			if seq < anchorSeq {
				delete(pending, seq)
				dropped++
			}
		}

		expectedSeq = anchorSeq
		missingSeqHoldCount = 0
		nextPlayoutAt = now.Add(chunkDur)
		metrics.CatchupResyncs++
		metrics.HardResyncs++
		lastSoftResyncAt = now
		lastHardResyncAt = now
		hardResyncWindowUntil = now.Add(hardResyncWindow)

		newQueueDelay := currentQueueDelay()
		lastDelayError = newQueueDelay - playoutDelay

		logInfof("gRPC stream: hard resync session=%q target=%s reason=%s dropped=%d expected_seq=%d->%d pending=%d->%d queue_delay=%s->%s delay_error=%s", req.SessionId, target, reason, dropped, oldExpected, expectedSeq, oldPending, len(pending), oldQueueDelay, newQueueDelay, lastDelayError)
	}

	logHealth := func(stage string) {
		queueDelay := currentQueueDelay()
		delayError := queueDelay - playoutDelay
		logDebugf("gRPC stream: health stage=%s session=%q target=%s received=%d played=%d late_dropped=%d duplicate_dropped=%d underflows=%d gap_silence=%d catchup_resyncs=%d hard_resyncs=%d buffered=%d expected_seq=%d queue_delay=%s delay_error=%s one_way=%s",
			stage,
			req.SessionId,
			target,
			metrics.ReceivedChunks,
			metrics.PlayedChunks,
			metrics.LateDropped,
			metrics.DuplicateDropped,
			metrics.Underflows,
			metrics.GapFillSilence,
			metrics.CatchupResyncs,
			metrics.HardResyncs,
			len(pending),
			expectedSeq,
			queueDelay,
			delayError,
			metrics.OneWaySummary(),
		)
	}

	tickerPeriod := chunkDur / 4
	if tickerPeriod < 5*time.Millisecond {
		tickerPeriod = 5 * time.Millisecond
	}
	ticker := time.NewTicker(tickerPeriod)
	defer ticker.Stop()

	drainReady := func(now time.Time) error {
		if !initialized {
			return nil
		}
		if !playoutStarted {
			if now.Before(startAt) {
				return nil
			}
			playoutStarted = true
			playoutStartedAt = now
			nextPlayoutAt = startAt
			logInfof("gRPC stream: playout started session=%q target=%s start_at=%s jitter_delay=%s requested_start=%v", req.SessionId, target, startAt.Format(time.RFC3339Nano), playoutDelay, requestedStart)
		}

		steps := 0
		for !nextPlayoutAt.After(now) {
			steps++
			if steps > maxPlayoutStepsPerDrain {
				metrics.CatchupResyncs++
				nextPlayoutAt = now.Add(chunkDur)
				logDebugf("gRPC stream: playout catch-up limited session=%q target=%s expected_seq=%d pending=%d", req.SessionId, target, expectedSeq, len(pending))
				break
			}

			if data, ok := pending[expectedSeq]; ok {
				delete(pending, expectedSeq)
				missingSeqHoldCount = 0
				metrics.PlayedChunks++
				if err := writeAudio(data, expectedSeq, "chunk"); err != nil {
					return err
				}
			} else {
				metrics.Underflows++
				metrics.GapFillSilence++
				if err := writeAudio(silenceChunk, expectedSeq, "silence-gap"); err != nil {
					return err
				}
				if eosSeen || missingSeqHoldCount >= missingSeqHoldChunks {
					expectedSeq++
					missingSeqHoldCount = 0
				} else {
					missingSeqHoldCount++
				}

				nextPlayoutAt = nextPlayoutAt.Add(chunkDur)

				if eosSeen && expectedSeq > endSeq && len(pending) == 0 {
					return nil
				}
				continue
			}

			expectedSeq++
			nextPlayoutAt = nextPlayoutAt.Add(chunkDur)

			if eosSeen && expectedSeq > endSeq && len(pending) == 0 {
				return nil
			}
		}

		return nil
	}

	for {
		if eosSeen && initialized && playoutStarted && expectedSeq > endSeq && len(pending) == 0 {
			break
		}

		select {
		case <-ticker.C:
			now := time.Now()
			if playoutStarted && initialized {
				queueDelay := currentQueueDelay()
				delayError := queueDelay - playoutDelay
				lastDelayError = delayError
				chunkBacklog := len(pending)
				inWarmup := !playoutStartedAt.IsZero() && now.Sub(playoutStartedAt) < resyncWarmup

				hardByDelay := delayError > hardResyncDelayThreshold
				hardByChunks := chunkBacklog >= hardResyncChunkThreshold

				if !inWarmup && (hardByDelay && hardByChunks) && hardResyncAllowed(now) {
					hardResync(now, fmt.Sprintf("delay_error=%s threshold=%s backlog_chunks=%d chunk_threshold=%d", delayError, hardResyncDelayThreshold, chunkBacklog, hardResyncChunkThreshold))
				} else if !inWarmup && now.After(hardResyncWindowUntil) && absoluteDuration(delayError) > softResyncBand {
					starving := delayError < 0 && chunkBacklog < minSoftResyncBufferedChunks
					if !starving && (lastSoftResyncAt.IsZero() || now.Sub(lastSoftResyncAt) >= softResyncCooldown) {
						magnitude := time.Duration(float64(absoluteDuration(delayError)) * softResyncGain)
						if magnitude < softResyncMinStep {
							magnitude = softResyncMinStep
						}
						if magnitude > softResyncMaxStep {
							magnitude = softResyncMaxStep
						}
						step := -magnitude
						if delayError < 0 {
							step = magnitude
						}
						nextPlayoutAt = nextPlayoutAt.Add(step)
						lastSoftResyncAt = now
						logDebugf("gRPC stream: soft resync nudge session=%q target=%s step=%s queue_delay=%s target_delay=%s delay_error=%s", req.SessionId, target, step, queueDelay, playoutDelay, delayError)
					}
				}
			}

			if adaptiveEnabled && playoutStarted && now.Sub(lastAdaptiveTuneAt) >= adaptiveTuneInterval {
				windowReceived := metrics.ReceivedChunks - lastAdaptiveReceived
				windowLate := metrics.LateDropped - lastAdaptiveLate
				windowUnderflows := metrics.Underflows - lastAdaptiveUnderflows
				windowCatchup := metrics.CatchupResyncs - lastAdaptiveCatchup

				newDelay := playoutDelay
				reason := ""
				if windowUnderflows > 0 || windowLate > 0 || windowCatchup > 0 {
					stableAdaptiveWindows = 0
					candidate := playoutDelay + adaptiveStep
					if candidate > adaptiveMax {
						candidate = adaptiveMax
					}
					if candidate != playoutDelay {
						newDelay = candidate
						reason = fmt.Sprintf("increase underflows=%d late=%d catchup=%d", windowUnderflows, windowLate, windowCatchup)
					}
				} else if windowReceived > 0 && currentQueueDelay() <= playoutDelay {
					stableAdaptiveWindows++
					if stableAdaptiveWindows >= adaptiveDecreaseStableWindows {
						candidate := playoutDelay - adaptiveStep
						if candidate < adaptiveMin {
							candidate = adaptiveMin
						}
						if candidate != playoutDelay {
							newDelay = candidate
							reason = fmt.Sprintf("decrease stable_windows=%d window_received=%d buffered=%d", stableAdaptiveWindows, windowReceived, len(pending))
						}
						stableAdaptiveWindows = 0
					}
				} else {
					stableAdaptiveWindows = 0
				}

				if newDelay != playoutDelay {
					delta := newDelay - playoutDelay
					playoutDelay = newDelay
					metrics.TargetJitterDelay = playoutDelay
					nextPlayoutAt = nextPlayoutAt.Add(delta)
					logDebugf("gRPC stream: adaptive jitter adjusted session=%q target=%s new_delay=%s min=%s max=%s step=%s reason=%s", req.SessionId, target, playoutDelay, adaptiveMin, adaptiveMax, adaptiveStep, reason)
				}

				lastAdaptiveTuneAt = now
				lastAdaptiveReceived = metrics.ReceivedChunks
				lastAdaptiveLate = metrics.LateDropped
				lastAdaptiveUnderflows = metrics.Underflows
				lastAdaptiveCatchup = metrics.CatchupResyncs
			}

			if now.Sub(lastHealthLogAt) >= healthLogInterval {
				lastHealthLogAt = now
				logHealth("periodic")
			}
			if playoutStarted && !lastChunkAt.IsZero() && time.Since(lastChunkAt) >= receiveStallLogInterval && time.Since(lastStallLogAt) >= receiveStallLogInterval {
				lastStallLogAt = time.Now()
				logInfof("gRPC stream: no chunks received for %s session=%q target=%s pending=%d expected_seq=%d", time.Since(lastChunkAt).Truncate(time.Millisecond), req.SessionId, target, len(pending), expectedSeq)
			}
			if err := drainReady(now); err != nil {
				logHealth("drain-error")
				return err
			}

		case env, ok := <-recvCh:
			if !ok {
				if err := drainReady(time.Now()); err != nil {
					logHealth("drain-error")
					return err
				}
				if eosSeen {
					for initialized && expectedSeq <= endSeq {
						if err := drainReady(time.Now().Add(chunkDur)); err != nil {
							logHealth("drain-error")
							return err
						}
					}
					break
				}
				logInfof("gRPC stream: receive channel closed without end-of-stream session=%q target=%s chunks_received=%d", req.SessionId, target, chunksReceived)
				logHealth("channel-closed")
				return io.EOF
			}

			if env.err != nil {
				if liveSink != nil {
					disableLiveSink("stream receive error", env.err)
				}
				if errors.Is(env.err, context.Canceled) || status.Code(env.err) == codes.Canceled {
					logInfof("gRPC stream: receive loop canceled target=%s session=%q chunks_received=%d", target, req.SessionId, chunksReceived)
					logHealth("canceled")
					return env.err
				}
				if errors.Is(env.err, io.EOF) {
					eosSeen = true
					if initialized {
						endSeq = expectedSeq + int64(len(pending))
					}
					continue
				}
				logErrorf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, req.SessionId, chunksReceived, env.err)
				logHealth("recv-error")
				return env.err
			}

			chunk := env.chunk
			if chunk == nil {
				continue
			}

			if chunk.GetEndOfStream() {
				eosSeen = true
				endSeq = chunk.GetSequence() - 1
				logInfof("gRPC stream: received final chunk target=%s session=%q final_seq=%d chunks_received=%d", target, req.SessionId, chunk.GetSequence(), chunksReceived)
				continue
			}

			chunksReceived++
			metrics.ReceivedChunks++
			lastChunkAt = env.at
			if !firstChunkLogged {
				firstChunkLogged = true
				firstArrival := env.at
				startAt = firstArrival.Add(playoutDelay)
				if requestedStart && localPlaybackAt.After(startAt) {
					startAt = localPlaybackAt
				}
				if requestedStart {
					logInfof("gRPC stream: follower first chunk session=%q target=%s at=%s requested_local_playback=%s chosen_playout_start=%s", req.SessionId, target, firstArrival.Format(time.RFC3339Nano), localPlaybackAt.Format(time.RFC3339Nano), startAt.Format(time.RFC3339Nano))
				} else {
					logInfof("gRPC stream: follower first chunk session=%q target=%s at=%s chosen_playout_start=%s", req.SessionId, target, firstArrival.Format(time.RFC3339Nano), startAt.Format(time.RFC3339Nano))
				}
			}

			if !initialized {
				expectedSeq = chunk.GetSequence()
				initialized = true
			}

			if chunk.GetSentAtNanos() > 0 {
				sentLocal := syncutil.ConvertSharedTimeToLocal(time.Unix(0, chunk.GetSentAtNanos()), offset)
				oneWay := env.at.Sub(sentLocal)
				if oneWay >= 0 {
					metrics.ObserveOneWay(oneWay)
				}
			}

			seq := chunk.GetSequence()
			if seq > latestSeqReceived {
				latestSeqReceived = seq
			}
			if seq < expectedSeq {
				metrics.LateDropped++
				continue
			}
			if _, exists := pending[seq]; exists {
				metrics.DuplicateDropped++
				continue
			}
			pending[seq] = append([]byte(nil), chunk.GetData()...)
			if depth := len(pending); depth > metrics.MaxBufferedChunks {
				metrics.MaxBufferedChunks = depth
			}
			if playoutStarted {
				queueDelay := currentQueueDelay()
				delayError := queueDelay - playoutDelay
				lastDelayError = delayError
				chunkBacklog := len(pending)
				inWarmup := !playoutStartedAt.IsZero() && time.Since(playoutStartedAt) < resyncWarmup
				if !inWarmup && (delayError >= hardResyncDelayThreshold*time.Duration(ingressHardResyncScale) && chunkBacklog >= hardResyncChunkThreshold*ingressHardResyncScale) && hardResyncAllowed(time.Now()) {
					hardResync(time.Now(), fmt.Sprintf("ingress backlog delay_error=%s backlog_chunks=%d", delayError, chunkBacklog))
				}
			}

			if err := drainReady(env.at); err != nil {
				logHealth("drain-error")
				return err
			}

			extra := fmt.Sprintf("buffered=%d queue_delay=%s delay_error=%s", len(pending), currentQueueDelay(), lastDelayError)
			s.logChunkEvent("buffered", req.SessionId, target, seq, len(chunk.GetData()), streamFormat.ChunkBytes, extra)
		}
	}

	logHealth("completed")

	if liveSink != nil {
		go func() {
			if err := closeLiveSink(); err != nil {
				logWarnf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
			}
		}()
	} else {
		playAt := startAt
		if playAt.IsZero() {
			playAt = time.Now()
		}
		go scheduleRawPlayback(playAt, outputPath, streamFormat)
	}

	logInfof("gRPC stream: leader stream finished target=%s session=%q chunks_received=%d output=%s", target, req.SessionId, chunksReceived, outputPath)
	return nil
}

func (s *peerControlServer) streamAudioToPeer(ctx context.Context, target, sessionID, audioID, audioPath string, sharedAt time.Time) error {
	logInfof("gRPC stream: opening client stream to target=%s session=%q audio_id=%q", target, sessionID, audioID)

	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		logWarnf("gRPC stream: failed to connect to target=%s session=%q: %v", target, sessionID, err)
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
		logWarnf("gRPC stream: failed to start client stream to target=%s session=%q: %v", target, sessionID, err)
		return err
	}

	chunksReceived := 0
	outputPath := fmt.Sprintf("/tmp/%s-%s.wav", sessionID, audioID)
	f, err := os.Create(outputPath)
	if err != nil {
		logErrorf("gRPC stream: failed to create output file %s: %v", outputPath, err)
		return err
	}

	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			f.Close()
			logInfof("gRPC stream: client stream finished target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(time.Now().Add(100*time.Millisecond), outputPath)
			return nil
		}
		if err != nil {
			f.Close()
			logErrorf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, sessionID, chunksReceived, err)
			return err
		}
		if chunk.EndOfStream {
			f.Close()
			logInfof("gRPC stream: received final chunk target=%s session=%q chunks_received=%d output=%s", target, sessionID, chunksReceived, outputPath)
			go schedulePlayback(time.Now().Add(100*time.Millisecond), outputPath)
			return nil
		}

		chunksReceived++
		if _, err := f.Write(chunk.Data); err != nil {
			f.Close()
			logErrorf("gRPC stream: failed to write chunk to %s: %v", outputPath, err)
			return err
		}

		logDebugf("gRPC stream: received chunk seq=%d size=%d target=%s session=%q", chunk.Sequence, len(chunk.Data), target, sessionID)
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

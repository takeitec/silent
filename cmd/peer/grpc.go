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

	id                          string
	isLeader                    bool
	pl                          *peerlist.PeerList
	grpcPort                    int
	wavPath                     string
	liveCapture                 bool
	captureDevice               string
	streamJitter                time.Duration
	streamJitterAdaptive        bool
	streamJitterSoftResync      bool
	streamJitterDriftCorrection bool
	streamJitterMin             time.Duration
	streamJitterMax             time.Duration
	streamJitterStep            time.Duration
	chunkLogStdoutMode          string
	chunkLogFileMode            string
	chunkLogEvery               int
	chunkLogFilePath            string
	chunkLogFile                *os.File
	chunkLogMu                  stdsync.Mutex
	offsetState                 *latestOffset
	sessionMu                   stdsync.Mutex
	activeSessions              map[string]time.Time
	sessionCancels              map[string]context.CancelFunc
	leaderStreams               map[string]*control.StreamPlaybackRequest
	leaderSharedStreams         map[string]*leaderSharedStream
}

const (
	chunkLogModeOff         = "off"
	chunkLogModeMilestone   = "milestone"
	chunkLogModeAll         = "all"
	leaderSharedRingSize    = 256
	leaderSlowDropWindow    = 5 * time.Second
	leaderSlowDropLimit     = 200
	leaderSlowRecoveryGrace = 5 * time.Second
	sinkWriteQueueCapacity  = 16
)

func normaliseChunkLogMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case chunkLogModeOff, chunkLogModeMilestone, chunkLogModeAll:
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return chunkLogModeMilestone
	}
}

func (s *peerControlServer) shouldLogChunk(mode string, seq int64, size, expectedSize int) bool {
	mode = normaliseChunkLogMode(mode)
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
	offset := s.currentOffset()

	localAt := syncutil.ConvertSharedTimeToLocal(sharedAt, offset)

	logInfof("received playback command for %s at local %s", req.AudioId, localAt.Format(time.RFC3339Nano))

	go schedulePlayback(localAt, req.AudioPath)

	return &control.PlaybackAck{Accepted: true}, nil
}

func (s *peerControlServer) currentOffset() time.Duration {
	if s == nil || s.offsetState == nil {
		return 0
	}
	offset, ok := s.offsetState.Get()
	if !ok {
		return 0
	}
	return offset
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
	streamReq := normaliseStreamRequest(req)
	streamFormat := normaliseStreamPlaybackRequest(streamReq)
	sessionID := normaliseSessionID(streamReq.SessionId)
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
	sessionID := normaliseSessionID(req.GetSessionId())
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
	sessionID := normaliseSessionID(req.GetSessionId())
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
	stopped, fanoutCount, stopErrors := s.stopLeaderSession(ctx, sessionID, reason)
	if stopped {
		logInfof("gRPC stream: leader stopped local session=%q", sessionID)
	} else {
		logInfof("gRPC stream: leader had no active local session=%q to stop", sessionID)
	}

	msg := fmt.Sprintf("stop broadcast to %d follower(s)", fanoutCount)
	if stopErrors > 0 {
		msg = fmt.Sprintf("%s with %d error(s)", msg, stopErrors)
	}
	logWarnf("gRPC stream: leader completed stop fanout for session=%q followers=%d errors=%d", sessionID, fanoutCount, stopErrors)
	return &control.StopStreamResponse{Accepted: stopErrors == 0, SessionId: sessionID, Message: msg}, nil
}

func (s *peerControlServer) stopLeaderSession(ctx context.Context, sessionID, reason string) (bool, int, int) {
	if !s.isLeader {
		return false, 0, 0
	}
	if _, ok := s.loadLeaderStream(sessionID); !ok {
		return false, 0, 0
	}

	s.clearLeaderStream(sessionID)
	s.closeLeaderSharedStream(sessionID, fmt.Errorf("%s", reason))
	logInfof("gRPC stream: leader beginning stop fanout for session=%q reason=%q", sessionID, reason)

	fanoutCount := 0
	stopErrors := 0
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

	s.finishSession(sessionID)
	logInfof("gRPC stream: leader stopped local session=%q", sessionID)
	return true, fanoutCount, stopErrors
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
	normalised := normaliseSessionID(sessionID)
	if _, active := s.sessionCancels[normalised]; active {
		return false
	}

	if expiry, ok := s.activeSessions[normalised]; ok {
		if now.Before(expiry) {
			return false
		}
	}

	s.activeSessions[normalised] = now.Add(sessionLease)
	return true
}

func (s *peerControlServer) finishSession(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	sessionID = normaliseSessionID(sessionID)
	delete(s.activeSessions, sessionID)
}

func normaliseSessionID(sessionID string) string {
	if strings.TrimSpace(sessionID) == "" {
		return "default"
	}
	return strings.TrimSpace(sessionID)
}

func normaliseStreamRequest(req *control.StreamPlaybackRequest) *control.StreamPlaybackRequest {
	if req == nil {
		req = &control.StreamPlaybackRequest{}
	}
	normalised := cloneStreamPlaybackRequest(req)
	normalised.SessionId = normaliseSessionID(normalised.SessionId)
	format := normaliseStreamPlaybackRequest(normalised)
	normalised.SampleRate = uint32(format.SampleRate)
	normalised.Channels = uint32(format.Channels)
	normalised.SampleFormat = format.SampleFormat
	return normalised
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
	normalised := normaliseSessionID(req.GetSessionId())
	s.leaderStreams[normalised] = cloneStreamPlaybackRequest(req)
	logInfof("gRPC stream: stored leader stream template session=%q", normalised)
}

func (s *peerControlServer) loadLeaderStream(sessionID string) (*control.StreamPlaybackRequest, bool) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalised := normaliseSessionID(sessionID)
	req, ok := s.leaderStreams[normalised]
	if !ok {
		return nil, false
	}
	return cloneStreamPlaybackRequest(req), true
}

func (s *peerControlServer) clearLeaderStream(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalised := normaliseSessionID(sessionID)
	if s.leaderStreams == nil {
		return
	}
	if _, ok := s.leaderStreams[normalised]; ok {
		delete(s.leaderStreams, normalised)
		logInfof("gRPC stream: cleared leader stream template session=%q", normalised)
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
	normalised := normaliseSessionID(sessionID)
	s.sessionCancels[normalised] = cancel
	logInfof("gRPC stream: registered cancellable session=%q", normalised)
}

func (s *peerControlServer) clearSessionCancel(sessionID string) {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()
	normalised := normaliseSessionID(sessionID)
	if _, ok := s.sessionCancels[normalised]; ok {
		delete(s.sessionCancels, normalised)
		logInfof("gRPC stream: cleared cancellable session=%q", normalised)
	}
}

func (s *peerControlServer) cancelSession(sessionID string) bool {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	normalised := normaliseSessionID(sessionID)
	cancel, ok := s.sessionCancels[normalised]
	if !ok {
		logInfof("gRPC stream: cancel requested for non-active session=%q", normalised)
		return false
	}
	delete(s.sessionCancels, normalised)
	logInfof("gRPC stream: canceling active session=%q", normalised)
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

type leaderSharedSubscriber struct {
	id                 string
	target             string
	nextSeq            int64
	done               chan struct{}
	dropWindowStart    time.Time
	dropWindowCount    int
	recoveryGraceUntil time.Time
	recoveryGraceUsed  bool
	errMu              stdsync.Mutex
	err                error
	closeOnce          stdsync.Once
}

func (sub *leaderSharedSubscriber) fail(err error) {
	sub.closeOnce.Do(func() {
		sub.errMu.Lock()
		sub.err = err
		sub.errMu.Unlock()
		close(sub.done)
	})
}

func (sub *leaderSharedSubscriber) failure() error {
	sub.errMu.Lock()
	defer sub.errMu.Unlock()
	return sub.err
}

type leaderSharedStream struct {
	sessionID   string
	format      streamFormat
	source      io.ReadCloser
	sourceName  string
	closeSource func() error

	mu       stdsync.Mutex
	cond     *stdsync.Cond
	seq      int64
	startSeq int64
	chunks   []*control.AudioChunk
	closed   bool
	lastErr  error
	subs     map[string]*leaderSharedSubscriber
	metrics  leaderSharedHealthMetrics
}

type leaderSharedHealthMetrics struct {
	ProducedChunks  int64
	RingEvictions   int64
	SubscriberSkips int64
	SlowDisconnects int64
	SendFailures    int64
	MaxBuffered     int
}

type leaderSharedSubscriberSnapshot struct {
	target          string
	lagChunks       int64
	lagDuration     time.Duration
	droppedInWindow int
	nextSeq         int64
}

func (ls *leaderSharedStream) snapshotHealth() (leaderSharedHealthMetrics, int, int64, int64, time.Duration, []leaderSharedSubscriberSnapshot) {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	snapshots := make([]leaderSharedSubscriberSnapshot, 0, len(ls.subs))
	chunkDur := streamChunkDuration(ls.format)
	for _, sub := range ls.subs {
		lagChunks := ls.seq - sub.nextSeq
		if lagChunks < 0 {
			lagChunks = 0
		}
		snapshots = append(snapshots, leaderSharedSubscriberSnapshot{
			target:          sub.target,
			lagChunks:       lagChunks,
			lagDuration:     time.Duration(lagChunks) * chunkDur,
			droppedInWindow: sub.dropWindowCount,
			nextSeq:         sub.nextSeq,
		})
	}
	return ls.metrics, len(ls.subs), ls.startSeq, ls.seq, time.Duration(len(ls.chunks)) * chunkDur, snapshots
}

func (ls *leaderSharedStream) logHealth(stage string) {
	metrics, subscribers, ringStart, ringEnd, retention, snapshots := ls.snapshotHealth()
	buffered := ringEnd - ringStart
	maxLagTarget := ""
	maxLagChunks := int64(0)
	maxLagDuration := time.Duration(0)
	for _, snapshot := range snapshots {
		if snapshot.lagChunks > maxLagChunks {
			maxLagChunks = snapshot.lagChunks
			maxLagDuration = snapshot.lagDuration
			maxLagTarget = snapshot.target
		}
	}
	logDebugf("gRPC stream: leader shared health stage=%s session=%q source=%s subscribers=%d produced=%d ring_start=%d ring_end=%d buffered=%d retention=%s max_buffered=%d ring_evictions=%d subscriber_skips=%d slow_disconnects=%d send_failures=%d slowest_target=%s slowest_lag_chunks=%d slowest_lag=%s",
		stage,
		ls.sessionID,
		ls.sourceName,
		subscribers,
		metrics.ProducedChunks,
		ringStart,
		ringEnd,
		buffered,
		retention,
		metrics.MaxBuffered,
		metrics.RingEvictions,
		metrics.SubscriberSkips,
		metrics.SlowDisconnects,
		metrics.SendFailures,
		maxLagTarget,
		maxLagChunks,
		maxLagDuration,
	)
	for _, snapshot := range snapshots {
		logDebugf("gRPC stream: leader subscriber health stage=%s session=%q target=%s next_seq=%d lag_chunks=%d lag=%s dropped_in_window=%d retention=%s",
			stage,
			ls.sessionID,
			snapshot.target,
			snapshot.nextSeq,
			snapshot.lagChunks,
			snapshot.lagDuration,
			snapshot.droppedInWindow,
			retention,
		)
	}
}

func (s *peerControlServer) getOrCreateLeaderSharedStream(req *control.StreamPlaybackRequest, format streamFormat) (*leaderSharedStream, error) {
	sessionID := normaliseSessionID(req.GetSessionId())

	s.sessionMu.Lock()
	if s.leaderSharedStreams == nil {
		s.leaderSharedStreams = make(map[string]*leaderSharedStream)
	}
	if existing, ok := s.leaderSharedStreams[sessionID]; ok {
		s.sessionMu.Unlock()
		return existing, nil
	}
	s.sessionMu.Unlock()

	source, sourceName, closeSource, err := s.openStreamSource(req)
	if err != nil {
		return nil, err
	}

	ls := &leaderSharedStream{
		sessionID:   sessionID,
		format:      format,
		source:      source,
		sourceName:  sourceName,
		closeSource: closeSource,
		chunks:      make([]*control.AudioChunk, 0, leaderSharedRingSize),
		subs:        make(map[string]*leaderSharedSubscriber),
	}
	ls.cond = stdsync.NewCond(&ls.mu)

	s.sessionMu.Lock()
	if existing, ok := s.leaderSharedStreams[sessionID]; ok {
		s.sessionMu.Unlock()
		if closeErr := closeSource(); closeErr != nil {
			logWarnf("gRPC stream: source close error (%s): %v", sourceName, closeErr)
		}
		return existing, nil
	}
	s.leaderSharedStreams[sessionID] = ls
	s.sessionMu.Unlock()

	logInfof("gRPC stream: leader shared source started session=%q source=%s", sessionID, sourceName)
	go s.runLeaderSharedProducer(req, ls)
	return ls, nil
}

func (s *peerControlServer) subscribeLeaderSharedStream(req *control.StreamPlaybackRequest, target string, format streamFormat) (*leaderSharedStream, *leaderSharedSubscriber, error) {
	ls, err := s.getOrCreateLeaderSharedStream(req, format)
	if err != nil {
		return nil, nil, err
	}

	sub := &leaderSharedSubscriber{
		id:     fmt.Sprintf("%s-%d", target, time.Now().UnixNano()),
		target: target,
		done:   make(chan struct{}),
	}

	ls.mu.Lock()
	if ls.closed {
		err := ls.lastErr
		ls.mu.Unlock()
		if err == nil {
			err = fmt.Errorf("leader shared stream closed")
		}
		return nil, nil, err
	}
	sub.nextSeq = ls.seq
	ls.subs[sub.id] = sub
	subscriberCount := len(ls.subs)
	bufferedChunks := len(ls.chunks)
	startSeq := ls.startSeq
	currentSeq := ls.seq
	ls.mu.Unlock()

	logInfof("gRPC stream: leader subscriber attached session=%q target=%s subscribers=%d next_seq=%d ring_start=%d ring_end=%d buffered=%d", ls.sessionID, target, subscriberCount, sub.nextSeq, startSeq, currentSeq, bufferedChunks)
	return ls, sub, nil
}

func (s *peerControlServer) unsubscribeLeaderSharedStream(ls *leaderSharedStream, sub *leaderSharedSubscriber, reason string) {
	if ls == nil || sub == nil {
		return
	}

	ls.mu.Lock()
	_, existed := ls.subs[sub.id]
	if existed {
		delete(ls.subs, sub.id)
	}
	subscriberCount := len(ls.subs)
	ls.mu.Unlock()

	if existed {
		logInfof("gRPC stream: leader subscriber detached session=%q target=%s reason=%s subscribers=%d", ls.sessionID, sub.target, reason, subscriberCount)
	}

	sub.fail(io.EOF)
	ls.cond.Broadcast()
}

func (s *peerControlServer) closeLeaderSharedStream(sessionID string, cause error) {
	normalized := normaliseSessionID(sessionID)

	s.sessionMu.Lock()
	ls, ok := s.leaderSharedStreams[normalized]
	if ok {
		delete(s.leaderSharedStreams, normalized)
	}
	s.sessionMu.Unlock()
	if !ok {
		return
	}

	ls.mu.Lock()
	if ls.closed {
		ls.mu.Unlock()
		return
	}
	ls.closed = true
	ls.lastErr = cause
	bufferedChunks := len(ls.chunks)
	subs := make([]*leaderSharedSubscriber, 0, len(ls.subs))
	for _, sub := range ls.subs {
		subs = append(subs, sub)
	}
	ls.subs = make(map[string]*leaderSharedSubscriber)
	ls.cond.Broadcast()
	ls.mu.Unlock()

	for _, sub := range subs {
		sub.fail(cause)
	}

	if closeErr := ls.closeSource(); closeErr != nil {
		logWarnf("gRPC stream: source close error (%s): %v", ls.sourceName, closeErr)
	}

	if cause != nil {
		logWarnf("gRPC stream: leader shared source closed session=%q reason=%v subscribers=%d buffered=%d", normalized, cause, len(subs), bufferedChunks)
	} else {
		logInfof("gRPC stream: leader shared source closed session=%q subscribers=%d buffered=%d", normalized, len(subs), bufferedChunks)
	}
}

func (s *peerControlServer) appendLeaderSharedChunk(ls *leaderSharedStream, chunk *control.AudioChunk) {
	ls.mu.Lock()
	if ls.closed {
		ls.mu.Unlock()
		return
	}
	if len(ls.chunks) == 0 {
		ls.startSeq = chunk.GetSequence()
	}
	if len(ls.chunks) >= leaderSharedRingSize {
		ls.chunks = ls.chunks[1:]
		ls.startSeq++
		ls.metrics.RingEvictions++
	}
	ls.chunks = append(ls.chunks, chunk)
	if !chunk.GetEndOfStream() {
		ls.metrics.ProducedChunks++
	}
	if len(ls.chunks) > ls.metrics.MaxBuffered {
		ls.metrics.MaxBuffered = len(ls.chunks)
	}
	ls.cond.Broadcast()
	ls.mu.Unlock()
}

func (s *peerControlServer) runLeaderSharedProducer(req *control.StreamPlaybackRequest, ls *leaderSharedStream) {
	buf := make([]byte, ls.format.ChunkBytes)
	chunkDur := streamChunkDuration(ls.format)
	paceStream := ls.sourceName != "live-capture" && chunkDur > 0
	nextSendAt := time.Time{}
	chunksSent := 0
	lastHealthLogAt := time.Now()
	const leaderSharedHealthLogInterval = 5 * time.Second

	failSession := func(err error) {
		ls.logHealth("producer-failed")
		s.closeLeaderSharedStream(ls.sessionID, err)
		s.stopLeaderSession(context.Background(), ls.sessionID, err.Error())
	}

	for {
		n, err := io.ReadFull(ls.source, buf)
		if n > 0 {
			producedAt := time.Now()
			if paceStream {
				now := time.Now()
				if nextSendAt.IsZero() {
					nextSendAt = now
				}
				if wait := time.Until(nextSendAt); wait > 0 {
					time.Sleep(wait)
				}
			}

			sentAt := time.Now()
			ls.mu.Lock()
			seq := ls.seq
			ls.seq++
			ls.mu.Unlock()

			chunk := &control.AudioChunk{
				SessionId:       req.SessionId,
				AudioId:         req.AudioId,
				Sequence:        seq,
				Data:            append([]byte(nil), buf[:n]...),
				SentAtNanos:     sentAt.UnixNano(),
				ProducedAtNanos: producedAt.UnixNano(),
				EndOfStream:     false,
			}

			s.appendLeaderSharedChunk(ls, chunk)
			chunksSent++
			if time.Since(lastHealthLogAt) >= leaderSharedHealthLogInterval {
				ls.logHealth("producer-periodic")
				lastHealthLogAt = time.Now()
			}
			if paceStream {
				nextSendAt = nextSendAt.Add(chunkDur)
			}
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			if ls.sourceName == "live-capture" {
				failSession(fmt.Errorf("live capture ended unexpectedly"))
				return
			}

			ls.mu.Lock()
			seq := ls.seq
			ls.seq++
			ls.mu.Unlock()
			eos := &control.AudioChunk{
				SessionId:   req.SessionId,
				AudioId:     req.AudioId,
				Sequence:    seq,
				SentAtNanos: time.Now().UnixNano(),
				EndOfStream: true,
			}
			s.appendLeaderSharedChunk(ls, eos)
			ls.logHealth("producer-completed")
			s.closeLeaderSharedStream(ls.sessionID, io.EOF)
			logInfof("gRPC stream: leader shared source finished session=%q chunks_sent=%d", ls.sessionID, chunksSent)
			return
		}
		if err != nil {
			failSession(fmt.Errorf("capture read failed: %w", err))
			return
		}
	}
}

func (s *peerControlServer) streamAudioFromLeaderShared(req *control.StreamPlaybackRequest, stream control.PeerControl_StreamAudioServer, target string, format streamFormat) error {
	ls, sub, err := s.subscribeLeaderSharedStream(req, target, format)
	if err != nil {
		return err
	}
	defer s.unsubscribeLeaderSharedStream(ls, sub, "stream-exit")

	for {
		if err := stream.Context().Err(); err != nil {
			return err
		}

		if err := sub.failure(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}

		ls.mu.Lock()
		for {
			if err := sub.failure(); err != nil {
				ls.mu.Unlock()
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}

			if sub.nextSeq < ls.startSeq {
				dropped := int(ls.startSeq - sub.nextSeq)
				now := time.Now()
				nextSequence := ls.startSeq
				sendDiscontinuity := func() error {
					marker := &control.AudioChunk{
						SessionId:             ls.sessionID,
						Sequence:              nextSequence,
						SequenceDiscontinuity: true,
						NextSequence:          nextSequence,
					}
					return stream.Send(marker)
				}
				if !sub.recoveryGraceUsed {
					sub.recoveryGraceUsed = true
					sub.recoveryGraceUntil = now.Add(leaderSlowRecoveryGrace)
					sub.dropWindowStart = now
					sub.dropWindowCount = 0
					ls.metrics.SubscriberSkips += int64(dropped)
					sub.nextSeq = ls.startSeq
					logWarnf("gRPC stream: leader subscriber recovery grace session=%q target=%s skipped=%d grace=%s", ls.sessionID, sub.target, dropped, leaderSlowRecoveryGrace)
					ls.mu.Unlock()
					if err := sendDiscontinuity(); err != nil {
						sub.fail(err)
						return err
					}
					ls.mu.Lock()
					continue
				}
				if now.Before(sub.recoveryGraceUntil) {
					sub.nextSeq = ls.startSeq
					ls.mu.Unlock()
					if err := sendDiscontinuity(); err != nil {
						sub.fail(err)
						return err
					}
					ls.mu.Lock()
					continue
				}
				if sub.dropWindowStart.IsZero() || now.Sub(sub.dropWindowStart) >= leaderSlowDropWindow {
					sub.dropWindowStart = now
					sub.dropWindowCount = 0
				}
				sub.dropWindowCount += dropped
				ls.metrics.SubscriberSkips += int64(dropped)
				logWarnf("gRPC stream: leader subscriber lagged session=%q target=%s skipped=%d next_seq=%d ring_start=%d ring_end=%d buffered=%d dropped_in_window=%d window=%s", ls.sessionID, sub.target, dropped, sub.nextSeq, ls.startSeq, ls.seq, len(ls.chunks), sub.dropWindowCount, leaderSlowDropWindow)
				sub.nextSeq = ls.startSeq
				if sub.dropWindowCount >= leaderSlowDropLimit {
					ls.metrics.SlowDisconnects++
					err := fmt.Errorf("slow subscriber dropped=%d window=%s", sub.dropWindowCount, leaderSlowDropWindow)
					ls.mu.Unlock()
					logWarnf("gRPC stream: leader disconnecting slow subscriber session=%q target=%s dropped_in_window=%d limit=%d", ls.sessionID, sub.target, sub.dropWindowCount, leaderSlowDropLimit)
					sub.fail(err)
					s.unsubscribeLeaderSharedStream(ls, sub, "slow-consumer")
					return err
				}
				ls.mu.Unlock()
				if err := sendDiscontinuity(); err != nil {
					sub.fail(err)
					return err
				}
				ls.mu.Lock()
			}

			available := int64(len(ls.chunks))
			if available > 0 {
				endSeq := ls.startSeq + available
				if sub.nextSeq >= ls.startSeq && sub.nextSeq < endSeq {
					idx := sub.nextSeq - ls.startSeq
					chunk := ls.chunks[idx]
					sub.nextSeq++
					ls.mu.Unlock()
					if chunk == nil {
						break
					}
					if err := stream.Send(chunk); err != nil {
						ls.mu.Lock()
						ls.metrics.SendFailures++
						ls.mu.Unlock()
						sub.fail(err)
						return err
					}
					if chunk.GetEndOfStream() {
						return nil
					}
					break
				}
			}

			if ls.closed {
				err := ls.lastErr
				ls.mu.Unlock()
				if err == nil || errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}

			ls.cond.Wait()
			if err := stream.Context().Err(); err != nil {
				ls.mu.Unlock()
				return err
			}
		}
	}
}

func (s *peerControlServer) StreamAudio(req *control.StreamPlaybackRequest, stream control.PeerControl_StreamAudioServer) error {
	target := streamTarget(stream.Context())
	streamFormat := normaliseStreamPlaybackRequest(req)
	logInfof("gRPC stream: server handler started session=%q audio_id=%q path=%q target=%s", req.SessionId, req.AudioId, req.AudioPath, target)
	if s.isLeader {
		return s.streamAudioFromLeaderShared(req, stream, target, streamFormat)
	}

	source, sourceName, closeSource, err := s.openStreamSource(req)
	if err != nil {
		if s.isLeader {
			s.stopLeaderSession(context.Background(), req.SessionId, err.Error())
		}
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
			producedAt := time.Now()
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

			sentAt := time.Now()
			chunk := &control.AudioChunk{
				SessionId:       req.SessionId,
				AudioId:         req.AudioId,
				Sequence:        seq,
				Data:            append([]byte(nil), buf[:n]...),
				SentAtNanos:     sentAt.UnixNano(),
				ProducedAtNanos: producedAt.UnixNano(),
				EndOfStream:     false,
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
			if sourceName == "live-capture" {
				err := fmt.Errorf("live capture ended unexpectedly")
				logWarnf("gRPC stream: live capture ended for session=%q chunks_sent=%d: %v", req.SessionId, chunksSent, err)
				if s.isLeader {
					s.stopLeaderSession(context.Background(), req.SessionId, err.Error())
				}
				return err
			}
			break
		}
		if err != nil {
			if s.isLeader {
				s.stopLeaderSession(context.Background(), req.SessionId, fmt.Sprintf("capture read failed: %v", err))
			}
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
	streamFormat := normaliseStreamPlaybackRequest(req)
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

	offset := s.currentOffset()

	localPlaybackAt := syncutil.ConvertSharedTimeToLocal(sharedAt, offset)
	playoutDelay := s.streamJitter
	if playoutDelay <= 0 {
		playoutDelay = 200 * time.Millisecond
	}
	adaptiveEnabled := s.streamJitterAdaptive
	softResyncEnabled := s.streamJitterSoftResync
	driftCorrectionEnabled := s.streamJitterDriftCorrection
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
	if !softResyncEnabled {
		logInfof("gRPC stream: soft resync disabled session=%q target=%s initial_delay=%s", req.SessionId, target, playoutDelay)
	}
	chunkDur := streamChunkDuration(streamFormat)
	silenceChunk := make([]byte, streamFormat.ChunkBytes)

	chunksReceived := 0
	firstChunkLogged := false
	metrics := newStreamHealthMetrics(playoutDelay)
	sinkWriteWindow := newDelaySummary()

	liveSink := io.WriteCloser(nil)
	closeLiveSink := func() error { return nil }
	liveLogPath := ""
	lastLiveErrorLogPath := ""
	restartAttempted := false
	startLiveSink := func(reason string) bool {
		sink, closeFn, logPath, err := startStreamingPlaybackWithFormatAndLog(streamFormat, req.SessionId)
		if err != nil {
			if reason == "initial" {
				logWarnf("gRPC stream: live playback unavailable for session=%q: %v", req.SessionId, err)
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
		msg := fmt.Sprintf("gRPC stream: live playback unavailable after retry for session=%q (trigger=%s)", req.SessionId, trigger)
		if lastLiveErrorLogPath != "" {
			msg = fmt.Sprintf("%s (ffplay log: %s)", msg, lastLiveErrorLogPath)
		}
		logInfof("%s", msg)
		return fmt.Errorf("%s", msg)
	}

	type queuedChunk struct {
		data       []byte
		receivedAt time.Time
		producedAt time.Time
		sentAt     time.Time
	}

	sinkCtx, cancelSink := context.WithCancel(ctx)
	defer cancelSink()
	sinkWriter := newSinkWriter(sinkCtx, sinkWriteQueueCapacity, func(request sinkWriteRequest) sinkWriteResult {
		result := sinkWriteResult{request: request}
		if liveSink == nil && !restartAttempted {
			attemptRestart("live sink unavailable before playout")
			if liveSink == nil {
				result.err = failDueToLivePlayback("startup failure")
				return result
			}
		}

		if liveSink != nil {
			result.startedAt = time.Now()
			_, writeErr := liveSink.Write(request.payload)
			result.duration = time.Since(result.startedAt)
			if writeErr != nil {
				trigger := fmt.Sprintf("live write (%s) seq=%d", request.source, request.seq)
				disableLiveSink(trigger, writeErr)
				attemptRestart(trigger)
				if liveSink == nil {
					result.err = failDueToLivePlayback(trigger)
				}
			}
		}
		return result
	}, func() {
		if liveSink == nil {
			return
		}
		if err := closeLiveSink(); err != nil {
			logWarnf("gRPC stream: live playback process ended with error for session=%q: %v", req.SessionId, err)
		}
	})

	lastSlowSinkWriteLogAt := time.Time{}
	lastSinkQueueDropLogAt := time.Time{}
	sinkResults := sinkWriter.results
	sinkQueueWait := newDelaySummary()
	handleSinkResult := func(result sinkWriteResult) error {
		if !result.request.enqueuedAt.IsZero() && !result.startedAt.IsZero() {
			sinkQueueWait.Observe(result.startedAt.Sub(result.request.enqueuedAt))
		}
		metrics.ObserveSinkWrite(result.duration)
		sinkWriteWindow.Observe(result.duration)
		if result.duration >= 100*time.Millisecond && (lastSlowSinkWriteLogAt.IsZero() || result.duration >= 2*time.Second || result.startedAt.Sub(lastSlowSinkWriteLogAt) >= 2*time.Second) {
			lastSlowSinkWriteLogAt = result.startedAt
			logWarnf("gRPC stream: slow live sink write session=%q seq=%d source=%s duration=%s payload_bytes=%d", req.SessionId, result.request.seq, result.request.source, result.duration, len(result.request.payload))
		}
		if result.err != nil {
			return result.err
		}
		if !result.startedAt.IsZero() {
			metrics.SinkWrittenChunks++
		}
		if !result.request.receivedAt.IsZero() && !result.request.scheduledAt.IsZero() {
			metrics.ObserveRecvToScheduled(result.request.scheduledAt.Sub(result.request.receivedAt))
			metrics.ObserveScheduledToWrite(result.startedAt.Sub(result.request.scheduledAt))
			if !result.request.producedAt.IsZero() {
				metrics.ObserveProducedToScheduled(result.request.scheduledAt.Sub(result.request.producedAt))
				metrics.ObserveProducedToWrite(result.startedAt.Sub(result.request.producedAt))
			}
		}
		return nil
	}

	writeAudio := func(payload []byte, seq int64, source string, scheduledAt time.Time, queued queuedChunk) error {
		if len(payload) == 0 {
			return nil
		}
		request := sinkWriteRequest{
			payload:     payload,
			seq:         seq,
			source:      source,
			enqueuedAt:  time.Now(),
			scheduledAt: scheduledAt,
			receivedAt:  queued.receivedAt,
			producedAt:  queued.producedAt,
		}
		dropped, accepted := sinkWriter.enqueue(request)
		if !accepted {
			return fmt.Errorf("live sink writer stopped")
		}
		if dropped != nil {
			metrics.SinkQueueDropped++
			now := time.Now()
			if lastSinkQueueDropLogAt.IsZero() || now.Sub(lastSinkQueueDropLogAt) >= 2*time.Second {
				lastSinkQueueDropLogAt = now
				logWarnf("gRPC stream: live sink queue overflow session=%q dropped_seq=%d dropped_source=%s queued_seq=%d capacity=%d dropped_total=%d", req.SessionId, dropped.seq, dropped.source, seq, sinkWriteQueueCapacity, metrics.SinkQueueDropped)
			}
		}
		return nil
	}

	type recvEnvelope struct {
		chunk      *control.AudioChunk
		err        error
		receivedAt time.Time
	}

	recvCh := make(chan recvEnvelope, 32)
	go func() {
		defer close(recvCh)
		for {
			chunk, recvErr := stream.Recv()
			recvCh <- recvEnvelope{chunk: chunk, err: recvErr, receivedAt: time.Now()}
			if recvErr != nil {
				return
			}
			if chunk.GetEndOfStream() {
				return
			}
		}
	}()

	pending := make(map[int64]queuedChunk)
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
	issueAdaptiveWindows := 0
	lastDelayError := time.Duration(0)
	hardResyncWindowUntil := time.Time{}
	lastHardResyncAt := time.Time{}
	latestSeqReceived := int64(-1)
	missingSeqHoldCount := 0
	ewmaDelayError := time.Duration(0)
	ewmaDelayErrorInitialized := false
	softResyncSignWindows := 0
	lastSoftResyncSign := 0
	lastAdaptiveControlAt := time.Time{}
	lastSoftControlAt := time.Time{}
	lastAdaptiveResetAt := time.Time{}
	controllerMode := "normal"
	playoutIntervalCorrection := time.Duration(0)
	driftCorrectionActive := false
	driftCorrectionPeak := time.Duration(0)
	driftCorrectionActiveSince := time.Time{}
	driftCorrectionActiveTotal := time.Duration(0)
	driftCorrectionEpisodes := 0
	driftEvidenceSampleAt := time.Time{}
	driftEvidenceBacklog := 0
	driftEvidenceSign := 0
	driftEvidenceWindows := 0
	pendingSoftNudgeAt := time.Time{}
	pendingSoftNudgeError := time.Duration(0)
	pendingSoftNudgeStep := time.Duration(0)
	prevBacklogSample := 0
	prevBacklogSampleAt := time.Time{}
	steepBacklogGrowth := false
	steepBacklogGrowthWindows := 0
	emergencyStartedAt := time.Time{}
	emergencyHealthySampleAt := time.Time{}
	emergencyHealthyWindows := 0

	const maxPlayoutStepsPerDrain = 8
	const receiveStallLogInterval = 2 * time.Second
	const healthLogInterval = 5 * time.Second
	const adaptiveTuneInterval = 5 * time.Second
	const adaptiveDecreaseStableWindows = 2
	const adaptiveIncreaseIssueWindows = 2
	const adaptiveResetOnSoftNudgeMinStep = 10 * time.Millisecond
	const adaptiveResetOnSoftNudgeMinInterval = 5 * time.Second
	// adaptiveEmergencyTuneInterval lets adaptive re-evaluate much faster than
	// adaptiveTuneInterval while the controller is in emergency mode, so it can
	// react to a stall or backlog blowup within a second or two instead of five.
	const adaptiveEmergencyTuneInterval = 1 * time.Second
	const emergencyMinimumDwell = 3 * time.Second
	const emergencyHealthySampleInterval = 250 * time.Millisecond
	const emergencyHealthyWindowsRequired = 3
	// emergencyStallThreshold/emergencyQueueDelayThreshold/backlogSteepGrowthChunksPerSec
	// define "emergency mode": conditions severe enough that the normal mutual
	// quiet-gating between soft resync and adaptive jitter should be bypassed so
	// whichever controller can react fastest is allowed to.
	const emergencyStallThreshold = 1200 * time.Millisecond
	const emergencyQueueDelayThreshold = 700 * time.Millisecond
	// backlogSteepGrowthChunksPerSec was raised from 40 to 80: live captures showed
	// a normal fill/drain sawtooth oscillating at ~45-56 chunks/sec (delta ~12-14
	// chunks per 250ms sample), which is well below genuine runaway spikes (~150-200+
	// chunks/sec) but was still tripping the old 40/s threshold every other sample.
	const backlogSteepGrowthChunksPerSec = 80.0
	// backlogSteepGrowthMinBacklogChunks requires the backlog to already be
	// meaningfully large before a fast rate counts as an emergency, so a rate
	// spike on a near-empty buffer doesn't trigger emergency mode.
	const backlogSteepGrowthMinBacklogChunks = 20
	// backlogSteepGrowthMinConsecutiveWindows requires the growth condition to
	// hold across back-to-back samples. This is the main defense against the
	// sawtooth oscillation, which alternates growth/shrink every sample and so
	// never satisfies two consecutive qualifying windows.
	const backlogSteepGrowthMinConsecutiveWindows = 2
	// backlogGrowthSampleInterval is the minimum time between backlog growth
	// samples. Recomputing the rate every 5ms ticker tick makes it noise-prone:
	// a single-chunk fluctuation over 5ms already looks like ~200 chunks/sec.
	// Sampling on a coarser interval requires growth to be sustained, not just
	// a momentary blip, before it's treated as an emergency.
	const backlogGrowthSampleInterval = 250 * time.Millisecond
	const emergencyHardResyncChunkDivisor = 3
	// adaptiveMinUnderflowRate guards against reacting to a single stray
	// underflow: only treat a window as "having a problem" if underflows
	// make up a meaningful share of chunks received in that window.
	const adaptiveMinUnderflowRate = 0.02 // 2%

	// adaptiveQuietAfterSoftResync suppresses adaptive target changes for a
	// short period after a soft resync nudge, so the adaptive controller
	// doesn't mistake a soft-resync-induced dip for a real network problem.
	const adaptiveQuietAfterSoftResync = 6000 * time.Millisecond

	// softResyncMaxStepLarge/ProportionalCeiling let the step scale up for
	// big errors instead of clamping every error above ~170ms to the same
	// tiny step.
	const softResyncMaxStepLarge = 60 * time.Millisecond
	const softResyncProportionalCeiling = 300 * time.Millisecond

	// softResyncQuietAfterAdaptive suppresses soft resync nudges right after
	// the adaptive controller moves the target, so the buffer gets a chance
	// to settle against the new target before we nudge playout timing again.
	const softResyncQuietAfterAdaptive = 4000 * time.Millisecond
	const emergencySoftResyncQuietAfterAdaptive = 1000 * time.Millisecond
	const emergencyAdaptiveQuietAfterSoftResync = 1000 * time.Millisecond
	const softResyncBand = 160 * time.Millisecond
	const recoveringEnterBand = 240 * time.Millisecond
	const recoveringExitBand = 120 * time.Millisecond
	const driftCorrectionMaxFraction = 0.001
	const driftCorrectionMinFraction = 0.0001
	const driftCorrectionExitBand = 80 * time.Millisecond
	const driftCorrectionWarmup = 10 * time.Second
	const driftEvidenceSampleInterval = 1 * time.Second
	const driftEvidenceRequiredWindows = 5
	const softNudgeVerificationDelay = 500 * time.Millisecond
	const softResyncMinStep = 8 * time.Millisecond
	const softResyncMaxStep = 12 * time.Millisecond
	const softResyncGain = 0.07
	const softResyncEWMAlpha = 0.2
	const softResyncConsecutiveWindows = 2
	const softResyncCooldown = 1400 * time.Millisecond
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

	playoutInterval := func() time.Duration {
		interval := chunkDur + playoutIntervalCorrection
		if interval <= 0 {
			return chunkDur
		}
		return interval
	}

	contiguousBufferedChunks := func() int {
		if !initialized {
			return 0
		}
		count := 0
		for seq := expectedSeq; ; seq++ {
			if _, ok := pending[seq]; !ok {
				break
			}
			count++
		}
		return count
	}

	absoluteDuration := func(v time.Duration) time.Duration {
		if v < 0 {
			return -v
		}
		return v
	}

	updateEWMA := func(raw time.Duration) time.Duration {
		if !ewmaDelayErrorInitialized {
			ewmaDelayError = raw
			ewmaDelayErrorInitialized = true
			return ewmaDelayError
		}
		ewma := softResyncEWMAlpha*float64(raw) + (1.0-softResyncEWMAlpha)*float64(ewmaDelayError)
		ewmaDelayError = time.Duration(ewma)
		return ewmaDelayError
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
		bufferedTotal := len(pending)
		queueDelayTotal := currentQueueDelay()
		delayErrorTotal := queueDelayTotal - playoutDelay
		bufferedPlayable := contiguousBufferedChunks()
		queueDelayPlayable := time.Duration(bufferedPlayable) * chunkDur
		delayErrorPlayable := queueDelayPlayable - playoutDelay
		driftCorrectionActiveTotalSnapshot := driftCorrectionActiveTotal
		if !driftCorrectionActiveSince.IsZero() {
			driftCorrectionActiveTotalSnapshot += time.Since(driftCorrectionActiveSince)
		}
		logDebugf("gRPC stream: health stage=%s session=%q target=%s received=%d playout_enqueued=%d sink_written=%d late_dropped=%d duplicate_dropped=%d underflows=%d gap_silence=%d catchup_resyncs=%d hard_resyncs=%d sink_queue_depth=%d sink_queue_capacity=%d sink_queue_dropped=%d buffered_total=%d buffered_playable=%d expected_seq=%d queue_delay_total=%s queue_delay_playable=%s delay_error_total=%s delay_error_playable=%s ewma_delay_error=%s playout_interval=%s drift_correction_peak=%s drift_correction_active_total=%s drift_correction_episodes=%d one_way=%s produced_to_recv=%s recv_to_scheduled=%s sink_queue_wait=%s scheduled_to_sink_write=%s produced_to_scheduled=%s produced_to_sink_write=%s sink_write=%s sink_write_window=%s send_block=%s",
			stage,
			req.SessionId,
			target,
			metrics.ReceivedChunks,
			metrics.PlayoutEnqueuedChunks,
			metrics.SinkWrittenChunks,
			metrics.LateDropped,
			metrics.DuplicateDropped,
			metrics.Underflows,
			metrics.GapFillSilence,
			metrics.CatchupResyncs,
			metrics.HardResyncs,
			len(sinkWriter.queue),
			sinkWriteQueueCapacity,
			metrics.SinkQueueDropped,
			bufferedTotal,
			bufferedPlayable,
			expectedSeq,
			queueDelayTotal,
			queueDelayPlayable,
			delayErrorTotal,
			delayErrorPlayable,
			ewmaDelayError,
			playoutInterval(),
			driftCorrectionPeak,
			driftCorrectionActiveTotalSnapshot,
			driftCorrectionEpisodes,
			metrics.OneWaySummary(),
			metrics.ProducedToRecvSummary(),
			metrics.RecvToScheduledSummary(),
			sinkQueueWait.Summary(),
			metrics.ScheduledToWriteSummary(),
			metrics.ProducedToScheduledSummary(),
			metrics.ProducedToWriteSummary(),
			metrics.SinkWriteSummary(),
			sinkWriteWindow.Summary(),
			metrics.SendBlockSummary(),
		)
		sinkWriteWindow = newDelaySummary()
		sinkQueueWait = newDelaySummary()
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

			if queued, ok := pending[expectedSeq]; ok {
				delete(pending, expectedSeq)
				missingSeqHoldCount = 0
				metrics.PlayoutEnqueuedChunks++
				if err := writeAudio(queued.data, expectedSeq, "chunk", nextPlayoutAt, queued); err != nil {
					return err
				}
			} else {
				metrics.Underflows++
				metrics.GapFillSilence++
				if err := writeAudio(silenceChunk, expectedSeq, "silence-gap", nextPlayoutAt, queuedChunk{}); err != nil {
					return err
				}
				if eosSeen || missingSeqHoldCount >= missingSeqHoldChunks {
					expectedSeq++
					missingSeqHoldCount = 0
				} else {
					missingSeqHoldCount++
				}

				nextPlayoutAt = nextPlayoutAt.Add(playoutInterval())

				if eosSeen && expectedSeq > endSeq && len(pending) == 0 {
					return nil
				}
				continue
			}

			expectedSeq++
			nextPlayoutAt = nextPlayoutAt.Add(playoutInterval())

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
		case result, ok := <-sinkResults:
			if ok {
				if err := handleSinkResult(result); err != nil {
					logHealth("sink-error")
					return err
				}
			} else {
				sinkResults = nil
			}
		case <-ticker.C:
			now := time.Now()
			if err := drainReady(now); err != nil {
				logHealth("drain-error")
				return err
			}
			if !pendingSoftNudgeAt.IsZero() && now.Sub(pendingSoftNudgeAt) >= softNudgeVerificationDelay {
				postNudgeError := currentQueueDelay() - playoutDelay
				errorDelta := postNudgeError - pendingSoftNudgeError
				logDebugf("gRPC stream: soft resync verification session=%q target=%s step=%s pre_error=%s post_error=%s error_delta=%s moved_toward_zero=%v", req.SessionId, target, pendingSoftNudgeStep, pendingSoftNudgeError, postNudgeError, errorDelta, absoluteDuration(postNudgeError) < absoluteDuration(pendingSoftNudgeError))
				pendingSoftNudgeAt = time.Time{}
			}

			emergency := false

			if playoutStarted && initialized {
				queueDelay := currentQueueDelay()
				delayErrorRaw := queueDelay - playoutDelay
				lastDelayError = delayErrorRaw
				delayErrorEWMA := updateEWMA(delayErrorRaw)
				chunkBacklog := len(pending)
				inWarmup := !playoutStartedAt.IsZero() && now.Sub(playoutStartedAt) < resyncWarmup

				isStalled := !lastChunkAt.IsZero() && now.Sub(lastChunkAt) >= emergencyStallThreshold
				isSevereDelay := delayErrorRaw >= emergencyQueueDelayThreshold

				if prevBacklogSampleAt.IsZero() || now.Sub(prevBacklogSampleAt) >= backlogGrowthSampleInterval {
					backlogGrowthRate := 0.0
					prevBacklogSampleLocal := prevBacklogSample
					backlogDelta := 0
					sampleDt := time.Duration(0)
					if !prevBacklogSampleAt.IsZero() {
						sampleDt = now.Sub(prevBacklogSampleAt)
						if sampleDt > 0 {
							backlogDelta = chunkBacklog - prevBacklogSample
							backlogGrowthRate = float64(backlogDelta) / sampleDt.Seconds()
						}
					}
					prevBacklogSample = chunkBacklog
					prevBacklogSampleAt = now

					candidateSteepBacklogGrowth := backlogGrowthRate > backlogSteepGrowthChunksPerSec && chunkBacklog >= backlogSteepGrowthMinBacklogChunks
					if candidateSteepBacklogGrowth {
						steepBacklogGrowthWindows++
					} else {
						steepBacklogGrowthWindows = 0
					}
					steepBacklogGrowth = steepBacklogGrowthWindows >= backlogSteepGrowthMinConsecutiveWindows
					if candidateSteepBacklogGrowth || steepBacklogGrowth {
						logDebugf("gRPC stream: backlog growth check session=%q target=%s prev=%d current=%d delta=%d dt_ms=%.1f rate=%.2f/s threshold=%.1f/s candidate=%v windows=%d steep=%v delay_error=%s", req.SessionId, target, prevBacklogSampleLocal, chunkBacklog, backlogDelta, sampleDt.Seconds()*1000, backlogGrowthRate, backlogSteepGrowthChunksPerSec, candidateSteepBacklogGrowth, steepBacklogGrowthWindows, steepBacklogGrowth, delayErrorRaw)
					}
				}

				rawEmergency := isStalled || isSevereDelay || steepBacklogGrowth
				emergency = rawEmergency
				if controllerMode == "emergency" && !emergencyStartedAt.IsZero() {
					if now.Sub(emergencyStartedAt) < emergencyMinimumDwell {
						emergency = true
						emergencyHealthyWindows = 0
					} else if !rawEmergency && absoluteDuration(delayErrorEWMA) <= recoveringExitBand && contiguousBufferedChunks() > 0 {
						if emergencyHealthySampleAt.IsZero() || now.Sub(emergencyHealthySampleAt) >= emergencyHealthySampleInterval {
							emergencyHealthySampleAt = now
							emergencyHealthyWindows++
						}
						emergency = emergencyHealthyWindows < emergencyHealthyWindowsRequired
					} else {
						emergencyHealthySampleAt = time.Time{}
						emergencyHealthyWindows = 0
						emergency = true
					}
				} else if rawEmergency {
					emergencyHealthySampleAt = time.Time{}
					emergencyHealthyWindows = 0
				}

				newMode := "normal"
				if emergency {
					newMode = "emergency"
				} else {
					recoveringBand := recoveringEnterBand
					if controllerMode == "recovering" {
						recoveringBand = recoveringExitBand
					}
					if chunkBacklog > 0 && absoluteDuration(delayErrorEWMA) > recoveringBand {
						newMode = "recovering"
					}
				}
				if newMode != controllerMode {
					if newMode == "emergency" || controllerMode == "emergency" {
						logInfof("gRPC stream: controller mode changed session=%q target=%s from=%s to=%s delay_error=%s backlog_chunks=%d stalled=%v severe_delay=%v steep_growth=%v", req.SessionId, target, controllerMode, newMode, delayErrorRaw, chunkBacklog, isStalled, isSevereDelay, steepBacklogGrowth)
					}
					if newMode == "emergency" {
						emergencyStartedAt = now
						emergencyHealthySampleAt = time.Time{}
						emergencyHealthyWindows = 0
						logInfof("gRPC stream: emergency started session=%q target=%s reason_stalled=%v reason_severe_delay=%v reason_steep_growth=%v delay_error=%s backlog_chunks=%d", req.SessionId, target, isStalled, isSevereDelay, steepBacklogGrowth, delayErrorRaw, chunkBacklog)
					} else if controllerMode == "emergency" && !emergencyStartedAt.IsZero() {
						logInfof("gRPC stream: emergency ended session=%q target=%s duration=%s next_mode=%s delay_error=%s backlog_chunks=%d", req.SessionId, target, now.Sub(emergencyStartedAt), newMode, delayErrorRaw, chunkBacklog)
						emergencyStartedAt = time.Time{}
					}
					controllerMode = newMode
				}

				hardByDelay := delayErrorRaw > hardResyncDelayThreshold
				hardByChunks := chunkBacklog >= hardResyncChunkThreshold
				if steepBacklogGrowth {
					// A steep backlog slope means the normal absolute thresholds will
					// arrive too late; react earlier once both signals are already trending badly.
					if delayErrorRaw > emergencyQueueDelayThreshold {
						hardByDelay = true
					}
					if chunkBacklog >= hardResyncChunkThreshold/emergencyHardResyncChunkDivisor {
						hardByChunks = true
					}
				}
				softResyncQuietAfterAdaptiveWindow := softResyncQuietAfterAdaptive
				if emergency {
					softResyncQuietAfterAdaptiveWindow = emergencySoftResyncQuietAfterAdaptive
				}
				softResyncQuiet := !lastAdaptiveControlAt.IsZero() && now.Sub(lastAdaptiveControlAt) < softResyncQuietAfterAdaptiveWindow

				if !inWarmup && (hardByDelay && hardByChunks) && hardResyncAllowed(now) {
					hardResync(now, fmt.Sprintf("delay_error=%s threshold=%s backlog_chunks=%d chunk_threshold=%d", delayErrorRaw, hardResyncDelayThreshold, chunkBacklog, hardResyncChunkThreshold))
				} else if softResyncEnabled && softResyncQuiet {
					logDebugf("gRPC stream: soft resync suppressed (quiet after adaptive jitter) session=%q target=%s delay_error=%s ewma_delay_error=%s backlog_chunks=%d", req.SessionId, target, delayErrorRaw, delayErrorEWMA, chunkBacklog)
					// Adaptive just moved the target; let the buffer settle against it
					// before nudging playout timing again.
				} else if softResyncEnabled && !inWarmup && now.After(hardResyncWindowUntil) && absoluteDuration(delayErrorEWMA) > softResyncBand {
					starving := delayErrorEWMA < 0 && chunkBacklog < minSoftResyncBufferedChunks
					if !starving && (lastSoftResyncAt.IsZero() || now.Sub(lastSoftResyncAt) >= softResyncCooldown) {
						sign := 1
						if delayErrorEWMA < 0 {
							sign = -1
						}
						if sign == lastSoftResyncSign {
							softResyncSignWindows++
						} else {
							lastSoftResyncSign = sign
							softResyncSignWindows = 1
						}

						if softResyncSignWindows >= softResyncConsecutiveWindows {
							errMag := absoluteDuration(delayErrorEWMA)
							var magnitude time.Duration
							if errMag <= softResyncProportionalCeiling {
								magnitude = time.Duration(float64(errMag) * softResyncGain)
								if magnitude < softResyncMinStep {
									magnitude = softResyncMinStep
								}
								if magnitude > softResyncMaxStep {
									magnitude = softResyncMaxStep
								}
							} else {
								// Scale further for errors beyond the normal band so a
								// large deviation corrects faster than a small one.
								magnitude = time.Duration(float64(errMag) * softResyncGain)
								if magnitude < softResyncMaxStep {
									magnitude = softResyncMaxStep
								}
								if magnitude > softResyncMaxStepLarge {
									magnitude = softResyncMaxStepLarge
								}
							}
							step := -magnitude
							if delayErrorEWMA < 0 {
								step = magnitude
							}
							nextPlayoutAt = nextPlayoutAt.Add(step)
							lastSoftResyncAt = now
							lastSoftControlAt = now
							pendingSoftNudgeAt = now
							pendingSoftNudgeError = delayErrorRaw
							pendingSoftNudgeStep = step

							// Soft nudges can temporarily perturb underflow/catchup counters.
							// Reset adaptive window baselines on meaningful nudges so the
							// next adaptive decision reflects post-nudge behavior.
							if absoluteDuration(step) >= adaptiveResetOnSoftNudgeMinStep {
								if lastAdaptiveResetAt.IsZero() || now.Sub(lastAdaptiveResetAt) >= adaptiveResetOnSoftNudgeMinInterval {
									lastAdaptiveReceived = metrics.ReceivedChunks
									lastAdaptiveLate = metrics.LateDropped
									lastAdaptiveUnderflows = metrics.Underflows
									lastAdaptiveCatchup = metrics.CatchupResyncs
									issueAdaptiveWindows = 0
									stableAdaptiveWindows = 0
									lastAdaptiveResetAt = now
									logDebugf("gRPC stream: adaptive window reset after soft nudge session=%q target=%s step=%s threshold=%s min_interval=%s", req.SessionId, target, step, adaptiveResetOnSoftNudgeMinStep, adaptiveResetOnSoftNudgeMinInterval)
								} else {
									logDebugf("gRPC stream: adaptive window reset suppressed (min interval) session=%q target=%s step=%s since_last=%s min_interval=%s", req.SessionId, target, step, now.Sub(lastAdaptiveResetAt), adaptiveResetOnSoftNudgeMinInterval)
								}
							}

							playableChunks := contiguousBufferedChunks()
							playableDelay := time.Duration(playableChunks) * chunkDur
							playableError := playableDelay - playoutDelay
							logDebugf("gRPC stream: soft resync nudge session=%q target=%s step=%s queue_delay_total=%s queue_delay_playable=%s target_delay=%s delay_error_total=%s delay_error_ewma=%s delay_error_playable=%s", req.SessionId, target, step, queueDelay, playableDelay, playoutDelay, delayErrorRaw, delayErrorEWMA, playableError)
						}
					}
				} else if softResyncEnabled {
					softResyncSignWindows = 0
					lastSoftResyncSign = 0
				}

				errorMagnitude := absoluteDuration(delayErrorEWMA)
				if !driftCorrectionEnabled {
					driftCorrectionActive = false
					playoutIntervalCorrection = 0
					driftEvidenceSampleAt = time.Time{}
					driftEvidenceWindows = 0
				} else if !driftCorrectionActive && now.Sub(playoutStartedAt) >= driftCorrectionWarmup {
					if driftEvidenceSampleAt.IsZero() || now.Sub(driftEvidenceSampleAt) >= driftEvidenceSampleInterval {
						driftSign := 0
						if delayErrorEWMA > softResyncBand {
							driftSign = 1
						} else if delayErrorEWMA < -softResyncBand {
							driftSign = -1
						}
						backlogDelta := chunkBacklog - driftEvidenceBacklog
						trendAgrees := (driftSign > 0 && backlogDelta > 0) || (driftSign < 0 && backlogDelta < 0)
						if driftSign != 0 && trendAgrees {
							if driftSign == driftEvidenceSign {
								driftEvidenceWindows++
							} else {
								driftEvidenceSign = driftSign
								driftEvidenceWindows = 1
							}
						} else {
							driftEvidenceSign = 0
							driftEvidenceWindows = 0
						}
						driftEvidenceBacklog = chunkBacklog
						driftEvidenceSampleAt = now
						if driftEvidenceWindows >= driftEvidenceRequiredWindows {
							driftCorrectionActive = true
						}
					}
				} else if !driftCorrectionActive && now.Sub(playoutStartedAt) < driftCorrectionWarmup {
					driftEvidenceSampleAt = time.Time{}
					driftEvidenceBacklog = chunkBacklog
					driftEvidenceSign = 0
					driftEvidenceWindows = 0
				}
				if driftCorrectionEnabled && driftCorrectionActive && errorMagnitude <= driftCorrectionExitBand {
					driftCorrectionActive = false
				}
				if driftCorrectionActive {
					excess := float64(errorMagnitude-driftCorrectionExitBand) / float64(softResyncProportionalCeiling-driftCorrectionExitBand)
					if excess < driftCorrectionMinFraction/driftCorrectionMaxFraction {
						excess = driftCorrectionMinFraction / driftCorrectionMaxFraction
					}
					if excess > 1 {
						excess = 1
					}
					correction := time.Duration(float64(chunkDur) * driftCorrectionMaxFraction * excess)
					if delayErrorEWMA > 0 {
						correction = -correction
					}
					playoutIntervalCorrection = correction
				} else {
					playoutIntervalCorrection = 0
				}
				if playoutIntervalCorrection != 0 {
					if driftCorrectionActiveSince.IsZero() {
						driftCorrectionActiveSince = now
						driftCorrectionEpisodes++
					}
					if absoluteDuration(playoutIntervalCorrection) > absoluteDuration(driftCorrectionPeak) {
						driftCorrectionPeak = playoutIntervalCorrection
					}
				} else if !driftCorrectionActiveSince.IsZero() {
					driftCorrectionActiveTotal += now.Sub(driftCorrectionActiveSince)
					driftCorrectionActiveSince = time.Time{}
				}
			}

			tuneInterval := adaptiveTuneInterval
			if emergency {
				tuneInterval = adaptiveEmergencyTuneInterval
			}
			if adaptiveEnabled && playoutStarted && now.Sub(lastAdaptiveTuneAt) >= tuneInterval {
				windowReceived := metrics.ReceivedChunks - lastAdaptiveReceived
				windowLate := metrics.LateDropped - lastAdaptiveLate
				windowUnderflows := metrics.Underflows - lastAdaptiveUnderflows
				windowCatchup := metrics.CatchupResyncs - lastAdaptiveCatchup

				// A handful of underflows out of hundreds of chunks is normal noise,
				// not evidence the buffer is too small. React to underflow *rate*.
				underflowRate := 0.0
				if windowReceived > 0 {
					underflowRate = float64(windowUnderflows) / float64(windowReceived)
				}
				hasIssue := (windowReceived > 0 && underflowRate > adaptiveMinUnderflowRate) || windowLate > 0 || windowCatchup > 0

				adaptiveQuietAfterSoftResyncWindow := adaptiveQuietAfterSoftResync
				if emergency {
					adaptiveQuietAfterSoftResyncWindow = emergencyAdaptiveQuietAfterSoftResync
				}
				adaptiveQuiet := !lastSoftControlAt.IsZero() && now.Sub(lastSoftControlAt) < adaptiveQuietAfterSoftResyncWindow

				// During an emergency, don't wait for two consecutive bad windows;
				// a single severe window is enough to justify raising the target now.
				requiredIssueWindows := adaptiveIncreaseIssueWindows
				if emergency {
					requiredIssueWindows = 1
				}

				newDelay := playoutDelay
				reason := ""
				if hasIssue {
					stableAdaptiveWindows = 0
					issueAdaptiveWindows++
					if issueAdaptiveWindows >= requiredIssueWindows {
						candidate := playoutDelay + adaptiveStep
						if candidate > adaptiveMax {
							candidate = adaptiveMax
						}
						if candidate != playoutDelay {
							newDelay = candidate
							reason = fmt.Sprintf("increase issue_windows=%d underflows=%d late=%d catchup=%d", issueAdaptiveWindows, windowUnderflows, windowLate, windowCatchup)
						}
						issueAdaptiveWindows = 0
					}
				} else if windowReceived > 0 && currentQueueDelay() <= playoutDelay {
					issueAdaptiveWindows = 0
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
					issueAdaptiveWindows = 0
				}

				if newDelay != playoutDelay && adaptiveQuiet {
					logDebugf("gRPC stream: adaptive jitter change suppressed (quiet after soft resync) session=%q target=%s candidate=%s current=%s reason=%s", req.SessionId, target, newDelay, playoutDelay, reason)
					newDelay = playoutDelay
				}
				if newDelay != playoutDelay {
					delta := newDelay - playoutDelay
					playoutDelay = newDelay
					metrics.TargetJitterDelay = playoutDelay
					nextPlayoutAt = nextPlayoutAt.Add(delta)
					lastAdaptiveControlAt = now
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

			if chunk.GetSequenceDiscontinuity() {
				nextSequence := chunk.GetNextSequence()
				for seq := range pending {
					if seq < nextSequence {
						delete(pending, seq)
					}
				}
				expectedSeq = nextSequence
				initialized = true
				latestSeqReceived = nextSequence - 1
				logWarnf("gRPC stream: sequence discontinuity target=%s session=%q expected_seq=%d next_sequence=%d pending=%d", target, req.SessionId, expectedSeq, nextSequence, len(pending))
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
			lastChunkAt = env.receivedAt
			if !firstChunkLogged {
				firstChunkLogged = true
				firstArrival := env.receivedAt
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

			seq := chunk.GetSequence()
			var sentLocal time.Time
			if chunk.GetSentAtNanos() > 0 {
				sentLocal = syncutil.ConvertSharedTimeToLocal(time.Unix(0, chunk.GetSentAtNanos()), offset)
				oneWay := env.receivedAt.Sub(sentLocal)
				if oneWay >= 0 {
					metrics.ObserveOneWay(oneWay)
				}
			}
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
			queued := queuedChunk{
				data:       append([]byte(nil), chunk.GetData()...),
				receivedAt: env.receivedAt,
				sentAt:     sentLocal,
			}
			if chunk.GetProducedAtNanos() > 0 {
				producedLocal := syncutil.ConvertSharedTimeToLocal(time.Unix(0, chunk.GetProducedAtNanos()), offset)
				queued.producedAt = producedLocal
				metrics.ObserveProducedToRecv(env.receivedAt.Sub(producedLocal))
				if !queued.sentAt.IsZero() {
					metrics.ObserveSendBlock(queued.sentAt.Sub(producedLocal))
				}
			}
			pending[seq] = queued
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

			if err := drainReady(env.receivedAt); err != nil {
				logHealth("drain-error")
				return err
			}

			playableChunks := contiguousBufferedChunks()
			extra := fmt.Sprintf("buffered_total=%d buffered_playable=%d queue_delay_total=%s queue_delay_playable=%s delay_error_total=%s", len(pending), playableChunks, currentQueueDelay(), time.Duration(playableChunks)*chunkDur, lastDelayError)
			s.logChunkEvent("buffered", req.SessionId, target, chunk.GetSequence(), len(chunk.GetData()), streamFormat.ChunkBytes, extra)
		}
	}

	logHealth("completed")

	logInfof("gRPC stream: leader stream finished target=%s session=%q chunks_received=%d", target, req.SessionId, chunksReceived)
	return nil
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

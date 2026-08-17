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
	streamCodec                 string
	opusBitrate                 int
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
	rejoinInitialBackoff    = 500 * time.Millisecond
	rejoinMaxBackoff        = 5 * time.Second
)

func isSlowSubscriberDisconnect(err error) bool {
	return err != nil && strings.Contains(strings.ToLower(err.Error()), "slow subscriber")
}

func normaliseChunkLogMode(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case chunkLogModeOff, chunkLogModeMilestone, chunkLogModeAll:
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return chunkLogModeMilestone
	}
}

func (s *peerControlServer) shouldLogChunk(mode string, seq int64, size, expectedSize int, codec string) bool {
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
		fixedSizeCodec := strings.TrimSpace(codec) == "" || strings.EqualFold(codec, "pcm")
		if fixedSizeCodec {
			return true
		}
	}
	return false
}

func (s *peerControlServer) logChunkEvent(direction, sessionID, target string, seq int64, size, expectedSize int, codec string, extra string) {
	stdoutShouldLog := s.shouldLogChunk(s.chunkLogStdoutMode, seq, size, expectedSize, codec)
	fileShouldLog := s.shouldLogChunk(s.chunkLogFileMode, seq, size, expectedSize, codec)
	if !stdoutShouldLog && !fileShouldLog {
		return
	}

	msg := fmt.Sprintf("gRPC stream: %s chunk seq=%d size=%d session=%q target=%s", direction, seq, size, sessionID, target)
	if expectedSize > 0 && size != expectedSize {
		msg = fmt.Sprintf("%s expected_size=%d", msg, expectedSize)
	}
	if strings.TrimSpace(extra) != "" {
		msg = fmt.Sprintf("%s %s", msg, strings.TrimSpace(extra))
	}

	if stdoutShouldLog {
		logInfof("%s", msg)
	}

	if fileShouldLog && s.chunkLogFile != nil {
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

func (s *peerControlServer) StartStreamPlayback(_ context.Context, req *control.StreamPlaybackRequest) (*control.StreamPlaybackResponse, error) {
	streamReq := normaliseStreamRequest(req)
	streamFormat := normaliseStreamPlaybackRequest(streamReq)
	sessionID := normaliseSessionID(streamReq.SessionId)
	// Only the leader decides the codec; followers must keep whatever the leader sent them.
	if s.isLeader {
		streamReq.PayloadCodec = s.streamCodec
	}
	logInfof("gRPC stream: StartStreamPlayback session=%q audio_id=%q audio_path=%q codec=%s shared_at=%s", sessionID, streamReq.AudioId, streamReq.AudioPath, streamReq.PayloadCodec, time.Unix(0, streamReq.SharedAtNanos).Format(time.RFC3339Nano))

	// If this peer is already handling the same session, reject the duplicate request.
	if !s.beginSession(sessionID) {
		logInfof("gRPC stream: ignoring duplicate session=%q", sessionID)
		return &control.StreamPlaybackResponse{
			Accepted:  true,
			SessionId: sessionID,
			Message:   "stream already in progress",
		}, nil
	}

	// After session admission, branch by role to keep each path easy to reason about.
	if !s.isLeader {
		return s.startStreamPlaybackAsFollower(sessionID, streamReq, streamFormat)
	}

	s.startStreamPlaybackAsLeader(sessionID, streamReq)

	return &control.StreamPlaybackResponse{
		Accepted:  true,
		SessionId: sessionID,
		Message:   "stream playback started",
	}, nil
}

func (s *peerControlServer) startStreamPlaybackAsFollower(sessionID string, streamReq *control.StreamPlaybackRequest, streamFormat streamFormat) (*control.StreamPlaybackResponse, error) {
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
		backoff := rejoinInitialBackoff
		for {
			err := s.receiveAudioFromLeader(runCtx, target, streamReq, sharedAt)
			if err == nil || !isSlowSubscriberDisconnect(err) {
				if err != nil {
					if errors.Is(err, context.Canceled) || status.Code(err) == codes.Canceled {
						logInfof("gRPC stream: follower stream stopped session=%q", sessionID)
						return
					}
					logWarnf("gRPC stream: follower failed to receive session=%q from leader: %v", sessionID, err)
				}
				return
			}

			// Slow-subscriber disconnects are retriable; use bounded exponential backoff.
			logWarnf("gRPC stream: follower rejoining after slow-subscriber disconnect session=%q target=%s retry_in=%s: %v", sessionID, target, backoff, err)
			timer := time.NewTimer(backoff)
			select {
			case <-runCtx.Done():
				if !timer.Stop() {
					<-timer.C
				}
				logInfof("gRPC stream: follower stream stopped during rejoin session=%q", sessionID)
				return
			case <-timer.C:
			}
			backoff *= 2
			if backoff > rejoinMaxBackoff {
				backoff = rejoinMaxBackoff
			}
		}

	}()

	return &control.StreamPlaybackResponse{
		Accepted:  true,
		SessionId: sessionID,
		Message:   "stream playback started",
	}, nil
}

func (s *peerControlServer) startStreamPlaybackAsLeader(sessionID string, streamReq *control.StreamPlaybackRequest) {
	s.storeLeaderStream(streamReq)

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

	// Keep the leader session lease briefly after kickoff so late-join and stop commands
	// have a stable session to target while followers are connecting.
	if kickoffCount == 0 {
		go func(sessionID string) {
			time.Sleep(leaderSessionReleaseCooldown)
			s.finishSession(sessionID)
			logInfof("gRPC stream: leader released session=%q after cooldown=%s (no followers)", sessionID, leaderSessionReleaseCooldown)
		}(sessionID)
	} else {
		go func(sessionID string, fanoutCount int) {
			kickoffWG.Wait()
			time.Sleep(leaderSessionReleaseCooldown)
			s.finishSession(sessionID)
			logInfof("gRPC stream: leader released session=%q after kickoff completion followers=%d cooldown=%s", sessionID, fanoutCount, leaderSessionReleaseCooldown)
		}(sessionID, kickoffCount)
	}
}

func (s *peerControlServer) JoinStreamPlayback(ctx context.Context, req *control.JoinStreamRequest) (*control.JoinStreamResponse, error) {
	sessionID := normaliseSessionID(req.GetSessionId())
	followerID := strings.TrimSpace(req.GetFollowerId())
	logInfof("gRPC stream: JoinStreamPlayback session=%q follower=%q leader=%v", sessionID, followerID, s.isLeader)

	// Keep the top-level handler as role routing, with details in focused helpers.
	if !s.isLeader {
		return s.joinStreamPlaybackAsFollower(ctx, sessionID, followerID, req)
	}

	return s.joinStreamPlaybackAsLeader(ctx, sessionID, followerID, req)
}

func (s *peerControlServer) joinStreamPlaybackAsFollower(ctx context.Context, sessionID, followerID string, req *control.JoinStreamRequest) (*control.JoinStreamResponse, error) {
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

func (s *peerControlServer) joinStreamPlaybackAsLeader(ctx context.Context, sessionID, followerID string, req *control.JoinStreamRequest) (*control.JoinStreamResponse, error) {
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

	// Late-join can target one follower or all; report attempted/succeeded for observability.
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

	// Route by role so stop behavior remains easy to follow and test.
	if !s.isLeader {
		return s.stopStreamPlaybackAsFollower(sessionID, reason), nil
	}

	return s.stopStreamPlaybackAsLeader(ctx, sessionID, reason), nil
}

func (s *peerControlServer) stopStreamPlaybackAsFollower(sessionID, reason string) *control.StopStreamResponse {
	logInfof("gRPC stream: follower received stop for session=%q reason=%q", sessionID, reason)
	stopped := s.cancelSession(sessionID)
	if stopped {
		s.finishSession(sessionID)
		logInfof("gRPC stream: follower stopped local session=%q", sessionID)
		return &control.StopStreamResponse{Accepted: true, SessionId: sessionID, Message: "session stopped"}
	}
	logInfof("gRPC stream: follower had no active local session=%q to stop", sessionID)
	return &control.StopStreamResponse{Accepted: false, SessionId: sessionID, Message: "session not active on follower"}
}

func (s *peerControlServer) stopStreamPlaybackAsLeader(ctx context.Context, sessionID, reason string) *control.StopStreamResponse {
	// The leader stop path always attempts local stop + fanout, then reports aggregate status.
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
	return &control.StopStreamResponse{Accepted: stopErrors == 0, SessionId: sessionID, Message: msg}
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
			s.logChunkEvent("sent", req.SessionId, target, seq, len(chunk.Data), streamFormat.ChunkBytes, req.GetPayloadCodec(), "")
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

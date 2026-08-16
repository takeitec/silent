package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"silent/internal/control"
	"silent/internal/models"
)

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
		PayloadCodec:  req.GetPayloadCodec(),
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

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	stdsync "sync"
	"time"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"silent/internal/control"
)

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
	closeStream func() error

	mu       stdsync.Mutex
	cond     *stdsync.Cond
	seq      int64
	startSeq int64
	chunks   []*control.AudioChunk
	closed   bool
	lastErr  error
	subs     map[string]*leaderSharedSubscriber
	metrics  leaderSharedHealthMetrics

	encoder audioEncoder
}

type leaderSharedHealthMetrics struct {
	ProducedChunks  int64
	RingEvictions   int64
	SubscriberSkips int64
	SlowDisconnects int64
	SendFailures    int64
	MaxBuffered     int
	PCMBytes        int64
	PayloadBytes    int64
	EncodeTotal     time.Duration
	EncodeMax       time.Duration
	EncodeErrors    int64
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
	averagePayloadBytes := int64(0)
	if metrics.ProducedChunks > 0 {
		averagePayloadBytes = metrics.PayloadBytes / metrics.ProducedChunks
	}
	averageEncodeDuration := time.Duration(0)
	if metrics.ProducedChunks > 0 {
		averageEncodeDuration = metrics.EncodeTotal / time.Duration(metrics.ProducedChunks)
	}
	payloadPercent := 0.0
	compressionRatio := 0.0
	if metrics.PCMBytes > 0 {
		payloadPercent = 100 * float64(metrics.PayloadBytes) / float64(metrics.PCMBytes)
		compressionRatio = float64(metrics.PCMBytes) / float64(metrics.PayloadBytes)
	}
	logDebugf("gRPC stream: leader shared health stage=%s session=%q source=%s subscribers=%d produced=%d pcm_bytes_total=%d payload_bytes_total=%d payload_bytes_avg=%d payload_percent=%.1f%% compression_ratio=%.2fx encode_avg=%s encode_max=%s encode_errors=%d ring_start=%d ring_end=%d buffered=%d retention=%s max_buffered=%d ring_evictions=%d subscriber_skips=%d slow_disconnects=%d send_failures=%d slowest_target=%s slowest_lag_chunks=%d slowest_lag=%s",
		stage,
		ls.sessionID,
		ls.sourceName,
		subscribers,
		metrics.ProducedChunks,
		metrics.PCMBytes,
		metrics.PayloadBytes,
		averagePayloadBytes,
		payloadPercent,
		compressionRatio,
		averageEncodeDuration,
		metrics.EncodeMax,
		metrics.EncodeErrors,
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

	var encoder audioEncoder
	codec := payloadCodec(req.GetPayloadCodec())
	if codec == payloadCodecOpus {
		encoder, err = newOpusEncoder(format.SampleRate, format.Channels)
		if err != nil {
			if closeErr := closeSource(); closeErr != nil {
				logWarnf("gRPC stream: source close error (%s): %v", sourceName, closeErr)
			}
			return nil, fmt.Errorf("create opus encoder: %w", err)
		}
	}
	logInfof("gRPC stream: leader encoding session=%q codec=%s", sessionID, codec)

	closeResources := func() error {
		sourceErr := closeSource()
		if encoder == nil {
			return sourceErr
		}
		return errors.Join(sourceErr, encoder.Close())
	}

	ls := &leaderSharedStream{
		sessionID:   sessionID,
		format:      format,
		source:      source,
		sourceName:  sourceName,
		closeStream: closeResources,
		chunks:      make([]*control.AudioChunk, 0, leaderSharedRingSize),
		subs:        make(map[string]*leaderSharedSubscriber),
		encoder:     encoder,
	}
	ls.cond = stdsync.NewCond(&ls.mu)

	s.sessionMu.Lock()
	if existing, ok := s.leaderSharedStreams[sessionID]; ok {
		s.sessionMu.Unlock()
		if closeErr := closeResources(); closeErr != nil {
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

	if closeErr := ls.closeStream(); closeErr != nil {
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
		ls.metrics.PayloadBytes += int64(len(chunk.GetData()))
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

		if n == len(buf) {
			payload := buf[:n]
			ls.mu.Lock()
			ls.metrics.PCMBytes += int64(len(payload))
			ls.mu.Unlock()
			if ls.encoder != nil {
				encodeStartedAt := time.Now()
				encoded, encodeErr := ls.encoder.EncodePCM(payload)
				encodeDuration := time.Since(encodeStartedAt)
				if encodeErr != nil {
					ls.mu.Lock()
					ls.metrics.EncodeErrors++
					ls.mu.Unlock()
					failSession(fmt.Errorf("PCM encode failed: %w", encodeErr))
					return
				}
				ls.mu.Lock()
				ls.metrics.EncodeTotal += encodeDuration
				if encodeDuration > ls.metrics.EncodeMax {
					ls.metrics.EncodeMax = encodeDuration
				}
				ls.mu.Unlock()
				payload = encoded
			}
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
				Data:            append([]byte(nil), payload...),
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

		if n > 0 && n < len(buf) {
			failSession(fmt.Errorf("partial PCM frame: got=%d want=%d: %w", n, len(buf), err))
			return
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

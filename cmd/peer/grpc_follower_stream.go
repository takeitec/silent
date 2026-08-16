package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"silent/internal/control"
	syncutil "silent/internal/sync"
)

type followerPlayoutInit struct {
	streamFormat           streamFormat
	offset                 time.Duration
	localPlaybackAt        time.Time
	playoutDelay           time.Duration
	adaptiveEnabled        bool
	softResyncEnabled      bool
	driftCorrectionEnabled bool
	adaptiveMin            time.Duration
	adaptiveMax            time.Duration
	adaptiveStep           time.Duration
	chunkDur               time.Duration
	silenceChunk           []byte
}

type queuedChunk struct {
	data       []byte
	receivedAt time.Time
	producedAt time.Time
	sentAt     time.Time
}

type followerLiveSink struct {
	streamFormat     streamFormat
	sessionID        string
	sink             io.WriteCloser
	closeSink        func() error
	logPath          string
	lastErrorLogPath string
	restartAttempted bool
	sinkDisabled     bool
}

func newFollowerLiveSink(streamFormat streamFormat, sessionID string) *followerLiveSink {
	ls := &followerLiveSink{
		streamFormat: streamFormat,
		sessionID:    sessionID,
		sink:         nil,
		closeSink:    func() error { return nil },
	}
	_ = ls.start("initial")
	return ls
}

func (ls *followerLiveSink) start(reason string) bool {
	sink, closeFn, logPath, err := startStreamingPlaybackWithFormatAndLog(ls.streamFormat, ls.sessionID)
	if err != nil {
		if reason == "initial" {
			logWarnf("gRPC stream: live playback unavailable for session=%q: %v", ls.sessionID, err)
		} else {
			logWarnf("gRPC stream: live playback restart failed for session=%q after %s: %v", ls.sessionID, reason, err)
		}
		return false
	}

	ls.sink = sink
	ls.closeSink = closeFn
	ls.logPath = logPath
	logInfof("gRPC stream: live playback process started session=%q log=%s", ls.sessionID, ls.logPath)
	return true
}

func (ls *followerLiveSink) disable(reason string, cause error) {
	if ls.sink == nil || ls.sinkDisabled {
		return
	}

	currentClose := ls.closeSink
	currentLogPath := ls.logPath
	ls.lastErrorLogPath = currentLogPath
	ls.sinkDisabled = true
	logTail := ffplayLogTail(currentLogPath)
	if currentLogPath != "" {
		logInfof("gRPC stream: disabling live playback for session=%q (%s): %v (ffplay log: %s)", ls.sessionID, reason, cause, currentLogPath)
	} else {
		logInfof("gRPC stream: disabling live playback for session=%q (%s): %v", ls.sessionID, reason, cause)
	}
	if logTail != "" {
		logInfof("gRPC stream: ffplay stderr tail for session=%q: %s", ls.sessionID, logTail)
	}
	_ = ls.sink.Close()
	ls.sink = nil
	ls.closeSink = func() error { return nil }
	ls.logPath = ""

	go func() {
		if err := currentClose(); err != nil {
			if currentLogPath != "" {
				logWarnf("gRPC stream: live playback process ended with error for session=%q: %v (ffplay log: %s)", ls.sessionID, err, currentLogPath)
			} else {
				logWarnf("gRPC stream: live playback process ended with error for session=%q: %v", ls.sessionID, err)
			}
		}
	}()
}

func (ls *followerLiveSink) attemptRestart(trigger string) {
	if ls.restartAttempted {
		return
	}
	ls.restartAttempted = true
	if ls.start(trigger) {
		ls.sinkDisabled = false
		logInfof("gRPC stream: live playback restart succeeded for session=%q after %s", ls.sessionID, trigger)
	}
}

func (ls *followerLiveSink) failDueToLivePlayback(trigger string) error {
	msg := fmt.Sprintf("gRPC stream: live playback unavailable after retry for session=%q (trigger=%s)", ls.sessionID, trigger)
	if ls.lastErrorLogPath != "" {
		msg = fmt.Sprintf("%s (ffplay log: %s)", msg, ls.lastErrorLogPath)
	}
	logInfof("%s", msg)
	return fmt.Errorf("%s", msg)
}

func (ls *followerLiveSink) writeRequest(request sinkWriteRequest) sinkWriteResult {
	result := sinkWriteResult{request: request}
	if ls.sink == nil && !ls.restartAttempted {
		ls.attemptRestart("live sink unavailable before playout")
		if ls.sink == nil {
			result.err = ls.failDueToLivePlayback("startup failure")
			return result
		}
	}

	if ls.sink != nil {
		result.startedAt = time.Now()
		_, writeErr := ls.sink.Write(request.payload)
		result.duration = time.Since(result.startedAt)
		if writeErr != nil {
			trigger := fmt.Sprintf("live write (%s) seq=%d", request.source, request.seq)
			ls.disable(trigger, writeErr)
			ls.attemptRestart(trigger)
			if ls.sink == nil {
				result.err = ls.failDueToLivePlayback(trigger)
			}
		}
	}

	return result
}

func (ls *followerLiveSink) closeActive() {
	if ls.sink == nil {
		return
	}
	if err := ls.closeSink(); err != nil {
		logWarnf("gRPC stream: live playback process ended with error for session=%q: %v", ls.sessionID, err)
	}
}

func openFollowerStreamClient(ctx context.Context, target string, req *control.StreamPlaybackRequest) (*grpc.ClientConn, control.PeerControl_StreamAudioClient, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, fmt.Errorf("connect to leader target=%s session=%q: %w", target, req.GetSessionId(), err)
	}

	client := control.NewPeerControlClient(conn)
	stream, err := client.StreamAudio(ctx, req)
	if err != nil {
		conn.Close()
		return nil, nil, fmt.Errorf("start stream from leader target=%s session=%q: %w", target, req.GetSessionId(), err)
	}

	return conn, stream, nil
}

func (s *peerControlServer) buildFollowerPlayoutInit(req *control.StreamPlaybackRequest, target string, sharedAt time.Time) followerPlayoutInit {
	streamFormat := normaliseStreamPlaybackRequest(req)
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

	return followerPlayoutInit{
		streamFormat:           streamFormat,
		offset:                 offset,
		localPlaybackAt:        localPlaybackAt,
		playoutDelay:           playoutDelay,
		adaptiveEnabled:        adaptiveEnabled,
		softResyncEnabled:      softResyncEnabled,
		driftCorrectionEnabled: driftCorrectionEnabled,
		adaptiveMin:            adaptiveMin,
		adaptiveMax:            adaptiveMax,
		adaptiveStep:           adaptiveStep,
		chunkDur:               chunkDur,
		silenceChunk:           make([]byte, streamFormat.ChunkBytes),
	}
}

func (s *peerControlServer) receiveAudioFromLeader(ctx context.Context, target string, req *control.StreamPlaybackRequest, sharedAt time.Time) error {
	logInfof("gRPC stream: follower opening stream from leader target=%s session=%q audio_id=%q", target, req.SessionId, req.AudioId)

	conn, stream, err := openFollowerStreamClient(ctx, target, req)
	if err != nil {
		logWarnf("gRPC stream: failed to open leader stream target=%s session=%q: %v", target, req.SessionId, err)
		return err
	}
	defer conn.Close()

	init := s.buildFollowerPlayoutInit(req, target, sharedAt)
	streamFormat := init.streamFormat
	offset := init.offset
	localPlaybackAt := init.localPlaybackAt
	playoutDelay := init.playoutDelay
	adaptiveEnabled := init.adaptiveEnabled
	softResyncEnabled := init.softResyncEnabled
	driftCorrectionEnabled := init.driftCorrectionEnabled
	adaptiveMin := init.adaptiveMin
	adaptiveMax := init.adaptiveMax
	adaptiveStep := init.adaptiveStep
	chunkDur := init.chunkDur
	silenceChunk := init.silenceChunk

	chunksReceived := 0
	firstChunkLogged := false
	metrics := newStreamHealthMetrics(playoutDelay)
	sinkWriteWindow := newDelaySummary()

	live := newFollowerLiveSink(streamFormat, req.SessionId)

	// Sink writer decouples playout scheduling from potentially blocking device writes.
	sinkCtx, cancelSink := context.WithCancel(ctx)
	defer cancelSink()
	sinkWriter := newSinkWriter(sinkCtx, sinkWriteQueueCapacity, live.writeRequest, live.closeActive)

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

	var decoder audioDecoder
	followerCodec := payloadCodec(req.GetPayloadCodec())
	if followerCodec == payloadCodecOpus {
		decoder, err = newOpusDecoder(streamFormat.SampleRate, streamFormat.Channels)
		if err != nil {
			return fmt.Errorf("create Opus decoder: %w", err)
		}
		defer decoder.Close()
	}
	logInfof("gRPC stream: follower decoding session=%q codec=%s", req.SessionId, followerCodec)

	// Dedicated receive goroutine keeps gRPC Recv off the timing-critical playout loop.
	recvCh := make(chan recvEnvelope, 32)
	go func() {
		defer close(recvCh)
		for {
			chunk, recvErr := stream.Recv()
			select {
			case recvCh <- recvEnvelope{chunk: chunk, err: recvErr, receivedAt: time.Now()}:
			case <-ctx.Done():
				return
			}
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

	const (
		maxPlayoutStepsPerDrain = 8
		missingSeqHoldChunks    = 6
		receiveStallLogInterval = 2 * time.Second
		healthLogInterval       = 5 * time.Second
	)

	const (
		adaptiveTuneInterval = 5 * time.Second
		// Emergency mode reevaluates adaptive jitter more frequently so it can
		// react to a stall or backlog blowup within a second or two.
		adaptiveEmergencyTuneInterval       = 1 * time.Second
		adaptiveDecreaseStableWindows       = 2
		adaptiveIncreaseIssueWindows        = 2
		adaptiveResetOnSoftNudgeMinStep     = 10 * time.Millisecond
		adaptiveResetOnSoftNudgeMinInterval = 5 * time.Second
		// Ignore a single stray underflow unless underflows make up a meaningful
		// share of chunks received in the adaptive window.
		adaptiveMinUnderflowRate = 0.02
		// Suppress adaptive target changes briefly after a soft-resync nudge so
		// the controller does not mistake the nudge for a network problem.
		adaptiveQuietAfterSoftResync = 6000 * time.Millisecond
	)

	const (
		emergencyMinimumDwell           = 3 * time.Second
		emergencyHealthySampleInterval  = 250 * time.Millisecond
		emergencyHealthyWindowsRequired = 3
		// These thresholds define emergency mode: severe conditions where the
		// normal quiet-gating between the controllers should be bypassed.
		emergencyStallThreshold               = 1200 * time.Millisecond
		emergencyQueueDelayThreshold          = 700 * time.Millisecond
		emergencyHardResyncChunkDivisor       = 3
		emergencySoftResyncQuietAfterAdaptive = 1000 * time.Millisecond
		emergencyAdaptiveQuietAfterSoftResync = 1000 * time.Millisecond
	)

	const (
		// A sustained backlog slope must be paired with a meaningful backlog so
		// normal fill/drain sawtooth behavior does not trigger emergency mode.
		backlogSteepGrowthChunksPerSec          = 80.0
		backlogSteepGrowthMinBacklogChunks      = 20
		backlogSteepGrowthMinConsecutiveWindows = 2
		backlogGrowthSampleInterval             = 250 * time.Millisecond
	)

	const (
		resyncWarmup                  = 3 * time.Second
		softResyncMaxStepLarge        = 60 * time.Millisecond
		softResyncProportionalCeiling = 300 * time.Millisecond
		softResyncQuietAfterAdaptive  = 4000 * time.Millisecond
		softResyncBand                = 160 * time.Millisecond
		recoveringEnterBand           = 240 * time.Millisecond
		recoveringExitBand            = 120 * time.Millisecond
		softNudgeVerificationDelay    = 500 * time.Millisecond
		softResyncMinStep             = 8 * time.Millisecond
		softResyncMaxStep             = 12 * time.Millisecond
		softResyncGain                = 0.07
		softResyncEWMAlpha            = 0.2
		softResyncConsecutiveWindows  = 2
		softResyncCooldown            = 1400 * time.Millisecond
		minSoftResyncBufferedChunks   = 4
		hardResyncDelayThreshold      = 1200 * time.Millisecond
		hardResyncChunkThreshold      = 90
		hardResyncCooldown            = 10 * time.Second
		hardResyncWindow              = 6 * time.Second
		hardResyncRetainChunks        = 3
		ingressHardResyncScale        = 4
	)

	const (
		driftCorrectionMaxFraction   = 0.001
		driftCorrectionMinFraction   = 0.0001
		driftCorrectionExitBand      = 80 * time.Millisecond
		driftCorrectionWarmup        = 10 * time.Second
		driftEvidenceSampleInterval  = 1 * time.Second
		driftEvidenceRequiredWindows = 5
	)

	// Derived queue/playout helpers used by the controller and health logging.
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
		logDebugf("gRPC stream: health stage=%s session=%q target=%s received=%d playout_enqueued=%d sink_written=%d late_dropped=%d duplicate_dropped=%d underflows=%d gap_silence=%d catchup_resyncs=%d hard_resyncs=%d sink_queue_depth=%d sink_queue_capacity=%d sink_queue_dropped=%d buffered_total=%d buffered_playable=%d expected_seq=%d queue_delay_total=%s queue_delay_playable=%s delay_error_total=%s delay_error_playable=%s ewma_delay_error=%s playout_interval=%s drift_correction_peak=%s drift_correction_active_total=%s drift_correction_episodes=%d one_way=%s produced_to_recv=%s recv_to_scheduled=%s decode=%s sink_queue_wait=%s scheduled_to_sink_write=%s produced_to_scheduled=%s produced_to_sink_write=%s sink_write=%s sink_write_window=%s send_block=%s",
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
			metrics.DecodeSummary(),
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

	// drainReady advances wall-clock playout and emits either queued audio or gap-fill silence.
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

	handleTick := func(now time.Time) error {
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

		return nil
	}

	handleRecvEnvelope := func(env recvEnvelope, ok bool) (bool, error) {
		if !ok {
			if err := drainReady(time.Now()); err != nil {
				logHealth("drain-error")
				return false, err
			}
			if eosSeen {
				for initialized && expectedSeq <= endSeq {
					if err := drainReady(time.Now().Add(chunkDur)); err != nil {
						logHealth("drain-error")
						return false, err
					}
				}
				return true, nil
			}
			logInfof("gRPC stream: receive channel closed without end-of-stream session=%q target=%s chunks_received=%d", req.SessionId, target, chunksReceived)
			logHealth("channel-closed")
			return false, io.EOF
		}

		if env.err != nil {
			if errors.Is(env.err, context.Canceled) || status.Code(env.err) == codes.Canceled {
				logInfof("gRPC stream: receive loop canceled target=%s session=%q chunks_received=%d", target, req.SessionId, chunksReceived)
				logHealth("canceled")
				return false, env.err
			}
			if errors.Is(env.err, io.EOF) {
				eosSeen = true
				if initialized {
					endSeq = expectedSeq + int64(len(pending))
				}
				return false, nil
			}
			logErrorf("gRPC stream: receive error target=%s session=%q chunks_received=%d: %v", target, req.SessionId, chunksReceived, env.err)
			logHealth("recv-error")
			return false, env.err
		}

		chunk := env.chunk
		if chunk == nil {
			return false, nil
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
			return false, nil
		}

		if chunk.GetEndOfStream() {
			eosSeen = true
			endSeq = chunk.GetSequence() - 1
			logInfof("gRPC stream: received final chunk target=%s session=%q final_seq=%d chunks_received=%d", target, req.SessionId, chunk.GetSequence(), chunksReceived)
			return false, nil
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
			return false, nil
		}
		if _, exists := pending[seq]; exists {
			metrics.DuplicateDropped++
			return false, nil
		}

		payload := chunk.GetData()
		if decoder != nil {
			decodeStartedAt := time.Now()
			payload, err = decoder.DecodePacket(payload)
			if err != nil {
				metrics.DecodeErrors++
				return false, fmt.Errorf("decode Opus seq=%d: %w", chunk.GetSequence(), err)
			}
			metrics.ObserveDecode(time.Since(decodeStartedAt))
		}

		queued := queuedChunk{
			data:       append([]byte(nil), payload...),
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
			return false, err
		}

		playableChunks := contiguousBufferedChunks()
		extra := fmt.Sprintf("buffered_total=%d buffered_playable=%d queue_delay_total=%s queue_delay_playable=%s delay_error_total=%s", len(pending), playableChunks, currentQueueDelay(), time.Duration(playableChunks)*chunkDur, lastDelayError)
		s.logChunkEvent("buffered", req.SessionId, target, chunk.GetSequence(), len(chunk.GetData()), streamFormat.ChunkBytes, req.GetPayloadCodec(), extra)
		return false, nil
	}

	tickerPeriod := chunkDur / 4
	if tickerPeriod < 5*time.Millisecond {
		tickerPeriod = 5 * time.Millisecond
	}
	ticker := time.NewTicker(tickerPeriod)
	defer ticker.Stop()

	for {
		if eosSeen && initialized && playoutStarted && expectedSeq > endSeq && len(pending) == 0 {
			break
		}

		// Main event loop multiplexes: sink completions, playout ticks, and inbound chunks.
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
			if err := handleTick(time.Now()); err != nil {
				return err
			}
		case env, ok := <-recvCh:
			streamComplete, err := handleRecvEnvelope(env, ok)
			if err != nil {
				return err
			}
			if streamComplete {
				// Re-check the loop completion guard after draining end-of-stream.
				continue
			}
		}
	}

	logHealth("completed")

	logInfof("gRPC stream: leader stream finished target=%s session=%q chunks_received=%d", target, req.SessionId, chunksReceived)
	return nil
}

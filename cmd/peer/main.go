package main

import (
	"flag"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"silent/internal/discovery"
	"silent/internal/models"
	"silent/internal/peerlist"
)

type config struct {
	id                          string
	broadcastIP                 string
	port                        int
	controlPort                 int
	grpcPort                    int
	leader                      bool
	wavPath                     string
	liveCapture                 bool
	captureDevice               string
	streamCodec                 string
	logMediaCmds                bool
	streamJitter                time.Duration
	streamJitterAdaptive        bool
	streamJitterSoftResync      bool
	streamJitterDriftCorrection bool
	streamJitterMin             time.Duration
	streamJitterMax             time.Duration
	streamJitterStep            time.Duration
	chunkLogStdoutMode          string
	chunkLogFileMode            string
	chunkLogDir                 string
	chunkLogEvery               int
	logOutput                   string
	logLevel                    string
	logDir                      string
	logFileName                 string
	logTimeFormat               string
	logPerSession               bool
	logSessionStamp             string
	room                        bool
	roomURL                     string
	advertiseHost               string
}

const (
	logOutputStdout = "stdout"
	logOutputFile   = "file"
	logOutputBoth   = "both"

	logTimeRFC3339Nano = "rfc3339nano"
	logTimeRFC3339     = "rfc3339"
)

func normaliseLogOutput(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case logOutputStdout, logOutputFile, logOutputBoth:
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return logOutputBoth
	}
}

func normaliseLogTimeFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case logTimeRFC3339:
		return time.RFC3339
	case logTimeRFC3339Nano:
		return time.RFC3339Nano
	default:
		return time.RFC3339Nano
	}
}

func normaliseLogLevel(value string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn", "warning":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.DebugLevel
	}
}

func normaliseStreamCodec(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "opus":
		return "opus"
	case "pcm":
		return "pcm"
	default:
		return "pcm"
	}
}

func appendSessionSuffix(fileName, suffix string) string {
	if strings.TrimSpace(fileName) == "" || strings.TrimSpace(suffix) == "" {
		return fileName
	}

	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	if base == "" {
		base = "log"
	}

	return fmt.Sprintf("%s-%s%s", base, suffix, ext)
}

func configureAppLogging(cfg config) (func(), error) {
	outputMode := normaliseLogOutput(cfg.logOutput)
	timeFormat := normaliseLogTimeFormat(cfg.logTimeFormat)
	logLevel := normaliseLogLevel(cfg.logLevel)

	outputPaths := make([]string, 0, 2)
	if outputMode == logOutputStdout || outputMode == logOutputBoth {
		outputPaths = append(outputPaths, "stdout")
	}

	if outputMode == logOutputFile || outputMode == logOutputBoth {
		logDir := strings.TrimSpace(cfg.logDir)
		if logDir == "" {
			logDir = "logs"
		}
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			return nil, fmt.Errorf("create log directory %q: %w", logDir, err)
		}

		fileName := strings.TrimSpace(cfg.logFileName)
		if fileName == "" {
			fileName = fmt.Sprintf("silent-peer-%s.log", sanitizeForFilename(cfg.id))
		}
		fileName = appendSessionSuffix(fileName, cfg.logSessionStamp)

		logPath := filepath.Join(logDir, fileName)
		outputPaths = append(outputPaths, logPath)
	}

	if len(outputPaths) == 0 {
		outputPaths = append(outputPaths, "stdout")
	}

	zapCfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			NameKey:        "logger",
			CallerKey:      "caller",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.TimeEncoderOfLayout(timeFormat),
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:       outputPaths,
		ErrorOutputPaths:  outputPaths,
		DisableCaller:     true,
		DisableStacktrace: true,
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build zap logger: %w", err)
	}

	undoStdLog := zap.RedirectStdLog(logger)
	zap.ReplaceGlobals(logger)

	cleanup := func() {
		undoStdLog()
		_ = logger.Sync()
	}

	if outputMode == logOutputFile || outputMode == logOutputBoth {
		if len(outputPaths) > 0 {
			logInfof("app log: output=%s level=%s file=%s", outputMode, logLevel.String(), outputPaths[len(outputPaths)-1])
		}
	} else {
		logInfof("app log: output=%s level=%s", outputMode, logLevel.String())
	}

	return cleanup, nil
}

func main() {
	cfg := parseFlags()

	logCleanup, err := configureAppLogging(cfg)
	if err != nil {
		logFatalf("configure logging: %v", err)
	}
	if logCleanup != nil {
		defer logCleanup()
	}

	app, err := newPeerApp(cfg)
	if err != nil {
		logFatalf("create peer app: %v", err)
	}

	if err := app.Run(); err != nil {
		logFatalf("peer app: %v", err)
	}
}

func parseFlags() config {
	id := flag.String("id", "peer-1", "node identifier")
	broadcastIP := flag.String("broadcast-ip", "255.255.255.255", "broadcast address for peer discovery/scheduling")
	port := flag.Int("port", 9999, "UDP discovery port")
	controlPortFlag := flag.Int("control-port", 10000, "UDP control port")
	grpcPort := flag.Int("grpc-port", 50051, "gRPC control port")
	leader := flag.Bool("leader", false, "act as the leader for scheduling")
	wavPath := flag.String("wav", "", "optional wav file to play")
	liveCapture := flag.Bool("live-capture", true, "capture live system audio on leader instead of reading audio-path file")
	captureDevice := flag.String("capture-device", "default", "system audio capture device (Linux PulseAudio/PipeWire monitor or Windows WASAPI endpoint, default auto device)")
	streamCodec := flag.String("stream-codec", "pcm", "stream codec to use for live capture (opus|pcm)")
	logMediaCmds := flag.Bool("log-media-cmds", true, "log ffmpeg/ffplay/aplay commands before execution and on failures")
	streamJitterMS := flag.Int("stream-jitter-ms", 200, "target jitter buffer delay for streamed playback in milliseconds")
	streamJitterAdaptive := flag.Bool("stream-jitter-adaptive", true, "adapt jitter buffer delay at runtime based on stream health")
	streamJitterSoftResync := flag.Bool("stream-jitter-soft-resync", true, "apply soft playout resync nudges alongside adaptive jitter")
	streamJitterDriftCorrection := flag.Bool("stream-jitter-drift-correction", true, "apply continuous playout-rate correction for persistent clock drift")
	streamJitterMinMS := flag.Int("stream-jitter-min-ms", 50, "minimum adaptive jitter delay in milliseconds")
	streamJitterMaxMS := flag.Int("stream-jitter-max-ms", 400, "maximum adaptive jitter delay in milliseconds")
	streamJitterStepMS := flag.Int("stream-jitter-step-ms", 20, "adaptive jitter adjustment step in milliseconds")
	chunkLogStdoutMode := flag.String("stream-chunk-log-stdout", "milestone", "chunk log verbosity to stdout: off|milestone|all")
	chunkLogFileMode := flag.String("stream-chunk-log-file", "off", "chunk log verbosity to file: off|milestone|all")
	chunkLogDir := flag.String("stream-chunk-log-dir", "logs", "directory for stream chunk log files")
	chunkLogEvery := flag.Int("stream-chunk-log-every", 100, "milestone interval for stream chunk logs")
	logOutput := flag.String("log-output", "both", "application log output mode: stdout|file|both")
	logLevel := flag.String("log-level", "debug", "application log level: debug|info|warn|error")
	logDir := flag.String("log-dir", "logs", "directory for application log files")
	logFileName := flag.String("log-file", "", "application log filename (default: silent-peer-<id>.log)")
	logTimeFormat := flag.String("log-time-format", "rfc3339nano", "application log timestamp format: rfc3339nano|rfc3339")
	logPerSession := flag.Bool("log-per-session", true, "append a per-run timestamp suffix to app and stream chunk log filenames")
	room := flag.Bool("room", true, "use room-based discovery instead of UDP broadcast")
	roomURL := flag.String("room-url", "http://127.0.0.1:9100", "room service base URL")
	advertiseHost := flag.String("advertise-host", "", "override the host advertised to other peers")
	flag.Parse()

	jitter := time.Duration(*streamJitterMS) * time.Millisecond
	if jitter < 50*time.Millisecond {
		jitter = 50 * time.Millisecond
	}
	if jitter > 1000*time.Millisecond {
		jitter = 1000 * time.Millisecond
	}

	adaptiveMin := time.Duration(*streamJitterMinMS) * time.Millisecond
	adaptiveMax := time.Duration(*streamJitterMaxMS) * time.Millisecond
	adaptiveStep := time.Duration(*streamJitterStepMS) * time.Millisecond
	if adaptiveMin < 50*time.Millisecond {
		adaptiveMin = 50 * time.Millisecond
	}
	if adaptiveMax < adaptiveMin {
		adaptiveMax = adaptiveMin
	}
	if adaptiveMax > 1500*time.Millisecond {
		adaptiveMax = 1500 * time.Millisecond
	}
	if adaptiveStep <= 0 {
		adaptiveStep = 20 * time.Millisecond
	}

	every := *chunkLogEvery
	if every <= 0 {
		every = 50
	}

	logSessionStamp := ""
	if *logPerSession {
		logSessionStamp = time.Now().UTC().Format("20060102T150405Z")
	}

	normalisedStreamCodec := normaliseStreamCodec(*streamCodec)

	return config{
		id:                          *id,
		broadcastIP:                 *broadcastIP,
		port:                        *port,
		controlPort:                 *controlPortFlag,
		grpcPort:                    *grpcPort,
		leader:                      *leader,
		wavPath:                     *wavPath,
		liveCapture:                 *liveCapture,
		captureDevice:               *captureDevice,
		streamCodec:                 normalisedStreamCodec,
		logMediaCmds:                *logMediaCmds,
		streamJitter:                jitter,
		streamJitterAdaptive:        *streamJitterAdaptive,
		streamJitterSoftResync:      *streamJitterSoftResync,
		streamJitterDriftCorrection: *streamJitterDriftCorrection,
		streamJitterMin:             adaptiveMin,
		streamJitterMax:             adaptiveMax,
		streamJitterStep:            adaptiveStep,
		chunkLogStdoutMode:          *chunkLogStdoutMode,
		chunkLogFileMode:            *chunkLogFileMode,
		chunkLogDir:                 *chunkLogDir,
		chunkLogEvery:               every,
		logOutput:                   *logOutput,
		logLevel:                    *logLevel,
		logDir:                      *logDir,
		logFileName:                 *logFileName,
		logTimeFormat:               *logTimeFormat,
		logPerSession:               *logPerSession,
		logSessionStamp:             logSessionStamp,
		room:                        *room,
		roomURL:                     *roomURL,
		advertiseHost:               *advertiseHost,
	}
}

type peerApp struct {
	cfg         config
	pl          *peerlist.PeerList
	ann         *discovery.Announcer
	listener    *net.UDPConn
	offsetState *latestOffset
	grpcServer  *peerControlServer
	roomClient  *discovery.Client
}

func newPeerApp(cfg config) (*peerApp, error) {
	broadcastAddr, err := validateBroadcastIP(cfg.broadcastIP)
	if err != nil {
		return nil, err
	}

	pl := peerlist.New()
	ann := discovery.NewAnnouncer(discovery.Config{
		ID:          cfg.id,
		Port:        cfg.port,
		Leader:      cfg.leader,
		BroadcastIP: broadcastAddr,
	})

	ann.SetSeenCallback(func(p models.Peer) {
		host := p.Address
		if h, _, err := net.SplitHostPort(p.Address); err == nil {
			host = h
		}
		pl.Add(p.ID, host, p.Role, 0)
		logInfof("discovered peer %s (%s) role=%s", p.ID, host, p.Role)
	})

	controlPort := effectiveControlPort(cfg)

	listener, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: controlPort,
	})
	if err != nil {
		return nil, err
	}
	logInfof("peer %s listening for control on udp:%d", cfg.id, controlPort)

	offsetState := &latestOffset{}

	chunkLogFilePath := ""
	var chunkLogFile *os.File
	fileMode := normaliseChunkLogMode(cfg.chunkLogFileMode)
	if fileMode != chunkLogModeOff {
		logDir := cfg.chunkLogDir
		if logDir == "" {
			logDir = "logs"
		}
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			logErrorf("chunk log: failed to create log directory %q: %v", logDir, err)
		} else {
			chunkFileName := fmt.Sprintf("silent-stream-chunks-%s.log", sanitizeForFilename(cfg.id))
			chunkFileName = appendSessionSuffix(chunkFileName, cfg.logSessionStamp)
			chunkLogFilePath = filepath.Join(logDir, chunkFileName)
			f, err := os.OpenFile(chunkLogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				logErrorf("chunk log: failed to open file %q: %v", chunkLogFilePath, err)
				chunkLogFilePath = ""
			} else {
				chunkLogFile = f
				logInfof("chunk log: file=%s mode=%s", chunkLogFilePath, fileMode)
			}
		}
	}

	return &peerApp{
		cfg:         cfg,
		pl:          pl,
		ann:         ann,
		listener:    listener,
		offsetState: offsetState,
		grpcServer: &peerControlServer{
			id:                          cfg.id,
			isLeader:                    cfg.leader,
			pl:                          pl,
			grpcPort:                    cfg.grpcPort,
			wavPath:                     cfg.wavPath,
			liveCapture:                 cfg.liveCapture,
			captureDevice:               cfg.captureDevice,
			streamCodec:                 cfg.streamCodec,
			streamJitter:                cfg.streamJitter,
			streamJitterAdaptive:        cfg.streamJitterAdaptive,
			streamJitterSoftResync:      cfg.streamJitterSoftResync,
			streamJitterDriftCorrection: cfg.streamJitterDriftCorrection,
			streamJitterMin:             cfg.streamJitterMin,
			streamJitterMax:             cfg.streamJitterMax,
			streamJitterStep:            cfg.streamJitterStep,
			chunkLogStdoutMode:          normaliseChunkLogMode(cfg.chunkLogStdoutMode),
			chunkLogFileMode:            fileMode,
			chunkLogEvery:               cfg.chunkLogEvery,
			chunkLogFilePath:            chunkLogFilePath,
			chunkLogFile:                chunkLogFile,
			offsetState:                 offsetState,
		},
	}, nil
}

func shouldProbeLeader(cfg config, leader *models.Peer) bool {
	if cfg.leader || leader == nil {
		return false
	}
	return true
}

func effectiveControlPort(cfg config) int {
	if cfg.controlPort > 0 {
		return cfg.controlPort
	}
	if cfg.port > 0 {
		return cfg.port + 1
	}
	return 0
}

func probePortForLeader(peer models.Peer, fallback int) int {
	if peer.ControlPort > 0 {
		return peer.ControlPort
	}
	if fallback > 0 {
		return fallback
	}
	return 0
}

func (a *peerApp) Run() error {
	defer a.listener.Close()
	if a.grpcServer != nil && a.grpcServer.chunkLogFile != nil {
		defer a.grpcServer.chunkLogFile.Close()
	}

	setMediaCommandLoggingEnabled(a.cfg.logMediaCmds)

	if err := validateMediaRuntime(a.cfg); err != nil {
		return fmt.Errorf("media preflight failed: %w", err)
	}

	if !a.cfg.room {
		go func() {
			for {
				if err := a.ann.Announce(); err != nil {
					logWarnf("announce failed: %v", err)
				}
				time.Sleep(1 * time.Second)
			}
		}()

		go func() {
			if err := a.ann.Start(); err != nil {
				logInfof("discovery stopped: %v", err)
			}
		}()
	}

	go func() {
		if err := handleControl(a.listener, a.cfg.leader); err != nil {
			logInfof("control loop stopped: %v", err)
		}
	}()

	go func() {
		if err := startGRPCServer(fmt.Sprintf("0.0.0.0:%d", a.cfg.grpcPort), a.grpcServer); err != nil {
			logInfof("gRPC server stopped: %v", err)
		}
	}()

	if a.cfg.leader {
		logInfo("leader mode enabled")
	}
	if a.cfg.room {
		roomURL := a.cfg.roomURL
		if roomURL == "" {
			roomURL = "http://127.0.0.1:9100"
		}

		a.roomClient = registerWithRoom(a.cfg, roomURL)

		go func() {
			if a.roomClient == nil {
				return
			}

			ticker := time.NewTicker(2 * time.Second)
			defer ticker.Stop()
			lastRoomStateSig := ""
			lastLeaderInfo := ""

			for range ticker.C {
				state, err := a.roomClient.RoomState()
				if err != nil {
					logWarnf("room state lookup failed: %v", err)
					continue
				}

				peerSigs := make([]string, 0, len(state.Peers))
				currentLeaderInfo := ""
				for _, p := range state.Peers {
					peerSigs = append(peerSigs, fmt.Sprintf("%s|%s|%s|%d", p.ID, p.Role, p.Address, p.ControlPort))
					if p.ID == state.Leader || strings.EqualFold(p.Role, string(models.RoleLeader)) {
						currentLeaderInfo = fmt.Sprintf("%s|%s", p.ID, p.Address)
					}
				}
				sort.Strings(peerSigs)
				roomStateSig := fmt.Sprintf("leader=%s peers=%d [%s]", state.Leader, len(state.Peers), strings.Join(peerSigs, ","))

				if roomStateSig != lastRoomStateSig {
					logInfof("room state: leader=%s peers=%d", state.Leader, len(state.Peers))
					lastRoomStateSig = roomStateSig
				}
				if currentLeaderInfo != "" && currentLeaderInfo != lastLeaderInfo {
					parts := strings.SplitN(currentLeaderInfo, "|", 2)
					if len(parts) == 2 {
						logInfof("leader is %s at %s", parts[0], parts[1])
					}
					lastLeaderInfo = currentLeaderInfo
				}

				a.pl.Reset()
				for _, p := range state.Peers {
					role := models.Role(p.Role)
					if p.ID == a.cfg.id {
						continue
					}
					a.pl.Add(p.ID, p.Address, role, p.ControlPort)
				}
			}
		}()
	}

	logInfof("peer %s listening on udp:%d (control:%d)", a.cfg.id, a.cfg.port, effectiveControlPort(a.cfg))

	go func() {
		if a.cfg.leader {
			return
		}

		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		lastLeaderDiscovery := ""

		for range ticker.C {
			leader := a.pl.Leader()
			currentLeaderDiscovery := ""
			if leader != nil {
				currentLeaderDiscovery = fmt.Sprintf("%s|%s", leader.ID, leader.Address)
			}

			if !shouldProbeLeader(a.cfg, leader) {
				if leader == nil {
					if lastLeaderDiscovery != "" {
						logInfof("follower %s has not discovered a leader yet", a.cfg.id)
					}
					lastLeaderDiscovery = ""
				}
				continue
			}

			if currentLeaderDiscovery != lastLeaderDiscovery {
				logInfof("follower %s discovered leader %s at %s", a.cfg.id, leader.ID, leader.Address)
				lastLeaderDiscovery = currentLeaderDiscovery
			}
			if err := probeLeader(*leader, a.cfg.id, probePortForLeader(*leader, effectiveControlPort(a.cfg)), a.offsetState); err != nil {
				logWarnf("clock sync failed: %v", err)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	a.ann.Stop()
	logInfo("shutting down")
	return nil
}

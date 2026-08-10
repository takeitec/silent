package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"silent/internal/discovery"
	"silent/internal/models"
	"silent/internal/peerlist"
)

type config struct {
	id                 string
	broadcastIP        string
	port               int
	controlPort        int
	grpcPort           int
	leader             bool
	wavPath            string
	liveCapture        bool
	captureDevice      string
	logMediaCmds       bool
	streamJitter       time.Duration
	chunkLogStdoutMode string
	chunkLogFileMode   string
	chunkLogDir        string
	chunkLogEvery      int
	room               bool
	roomURL            string
	advertiseHost      string
}

func main() {
	cfg := parseFlags()

	app, err := newPeerApp(cfg)
	if err != nil {
		log.Fatalf("create peer app: %v", err)
	}

	if err := app.Run(); err != nil {
		log.Fatalf("peer app: %v", err)
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
	logMediaCmds := flag.Bool("log-media-cmds", true, "log ffmpeg/ffplay/aplay commands before execution and on failures")
	streamJitterMS := flag.Int("stream-jitter-ms", 200, "target jitter buffer delay for streamed playback in milliseconds")
	chunkLogStdoutMode := flag.String("stream-chunk-log-stdout", "milestone", "chunk log verbosity to stdout: off|milestone|all")
	chunkLogFileMode := flag.String("stream-chunk-log-file", "all", "chunk log verbosity to file: off|milestone|all")
	chunkLogDir := flag.String("stream-chunk-log-dir", "logs", "directory for stream chunk log files")
	chunkLogEvery := flag.Int("stream-chunk-log-every", 50, "milestone interval for stream chunk logs")
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

	every := *chunkLogEvery
	if every <= 0 {
		every = 50
	}

	return config{
		id:                 *id,
		broadcastIP:        *broadcastIP,
		port:               *port,
		controlPort:        *controlPortFlag,
		grpcPort:           *grpcPort,
		leader:             *leader,
		wavPath:            *wavPath,
		liveCapture:        *liveCapture,
		captureDevice:      *captureDevice,
		logMediaCmds:       *logMediaCmds,
		streamJitter:       jitter,
		chunkLogStdoutMode: *chunkLogStdoutMode,
		chunkLogFileMode:   *chunkLogFileMode,
		chunkLogDir:        *chunkLogDir,
		chunkLogEvery:      every,
		room:               *room,
		roomURL:            *roomURL,
		advertiseHost:      *advertiseHost,
	}
}

type peerApp struct {
	cfg        config
	pl         *peerlist.PeerList
	ann        *discovery.Announcer
	listener   *net.UDPConn
	offsetCh   chan time.Duration
	grpcServer *peerControlServer
	roomClient *discovery.Client
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
		log.Printf("discovered peer %s (%s) role=%s", p.ID, host, p.Role)
	})

	controlPort := effectiveControlPort(cfg)

	listener, err := net.ListenUDP("udp4", &net.UDPAddr{
		IP:   net.ParseIP("0.0.0.0"),
		Port: controlPort,
	})
	if err != nil {
		return nil, err
	}
	log.Printf("peer %s listening for control on udp:%d", cfg.id, controlPort)

	offsetCh := make(chan time.Duration, 1)

	chunkLogFilePath := ""
	var chunkLogFile *os.File
	fileMode := normalizeChunkLogMode(cfg.chunkLogFileMode)
	if fileMode != chunkLogModeOff {
		logDir := cfg.chunkLogDir
		if logDir == "" {
			logDir = "logs"
		}
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			log.Printf("chunk log: failed to create log directory %q: %v", logDir, err)
		} else {
			chunkLogFilePath = filepath.Join(logDir, fmt.Sprintf("silent-stream-chunks-%s.log", sanitizeForFilename(cfg.id)))
			f, err := os.OpenFile(chunkLogFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				log.Printf("chunk log: failed to open file %q: %v", chunkLogFilePath, err)
				chunkLogFilePath = ""
			} else {
				chunkLogFile = f
				log.Printf("chunk log: file=%s mode=%s", chunkLogFilePath, fileMode)
			}
		}
	}

	return &peerApp{
		cfg:      cfg,
		pl:       pl,
		ann:      ann,
		listener: listener,
		offsetCh: offsetCh,
		grpcServer: &peerControlServer{
			id:                 cfg.id,
			isLeader:           cfg.leader,
			pl:                 pl,
			grpcPort:           cfg.grpcPort,
			wavPath:            cfg.wavPath,
			liveCapture:        cfg.liveCapture,
			captureDevice:      cfg.captureDevice,
			streamJitter:       cfg.streamJitter,
			chunkLogStdoutMode: normalizeChunkLogMode(cfg.chunkLogStdoutMode),
			chunkLogFileMode:   fileMode,
			chunkLogEvery:      cfg.chunkLogEvery,
			chunkLogFilePath:   chunkLogFilePath,
			chunkLogFile:       chunkLogFile,
			offsetCh:           offsetCh,
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
					log.Printf("announce failed: %v", err)
				}
				time.Sleep(1 * time.Second)
			}
		}()

		go func() {
			if err := a.ann.Start(); err != nil {
				log.Printf("discovery stopped: %v", err)
			}
		}()
	}

	go func() {
		if err := handleControl(a.listener, a.cfg.leader); err != nil {
			log.Printf("control loop stopped: %v", err)
		}
	}()

	go func() {
		if err := startGRPCServer(fmt.Sprintf("0.0.0.0:%d", a.cfg.grpcPort), a.grpcServer); err != nil {
			log.Printf("gRPC server stopped: %v", err)
		}
	}()

	if a.cfg.leader {
		fmt.Println("leader mode enabled")
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
					log.Printf("room state lookup failed: %v", err)
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
					log.Printf("room state: leader=%s peers=%d", state.Leader, len(state.Peers))
					lastRoomStateSig = roomStateSig
				}
				if currentLeaderInfo != "" && currentLeaderInfo != lastLeaderInfo {
					parts := strings.SplitN(currentLeaderInfo, "|", 2)
					if len(parts) == 2 {
						log.Printf("leader is %s at %s", parts[0], parts[1])
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

	fmt.Printf("peer %s listening on udp:%d (control:%d)\n", a.cfg.id, a.cfg.port, effectiveControlPort(a.cfg))

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
						log.Printf("follower %s has not discovered a leader yet", a.cfg.id)
					}
					lastLeaderDiscovery = ""
				}
				continue
			}

			if currentLeaderDiscovery != lastLeaderDiscovery {
				log.Printf("follower %s discovered leader %s at %s", a.cfg.id, leader.ID, leader.Address)
				lastLeaderDiscovery = currentLeaderDiscovery
			}
			if err := probeLeader(*leader, a.cfg.id, probePortForLeader(*leader, effectiveControlPort(a.cfg)), a.offsetCh); err != nil {
				log.Printf("clock sync failed: %v", err)
			}
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	a.ann.Stop()
	fmt.Println("shutting down")
	return nil
}

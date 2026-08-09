package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"silent/internal/discovery"
	"silent/internal/models"
	"silent/internal/peerlist"
)

type config struct {
	id            string
	broadcastIP   string
	port          int
	controlPort   int
	grpcPort      int
	leader        bool
	wavPath       string
	room          bool
	roomURL       string
	advertiseHost string
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
	room := flag.Bool("room", true, "use room-based discovery instead of UDP broadcast")
	roomURL := flag.String("room-url", "http://127.0.0.1:9100", "room service base URL")
	advertiseHost := flag.String("advertise-host", "", "override the host advertised to other peers")
	flag.Parse()

	return config{
		id:            *id,
		broadcastIP:   *broadcastIP,
		port:          *port,
		controlPort:   *controlPortFlag,
		grpcPort:      *grpcPort,
		leader:        *leader,
		wavPath:       *wavPath,
		room:          *room,
		roomURL:       *roomURL,
		advertiseHost: *advertiseHost,
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

	return &peerApp{
		cfg:      cfg,
		pl:       pl,
		ann:      ann,
		listener: listener,
		offsetCh: offsetCh,
		grpcServer: &peerControlServer{
			id:       cfg.id,
			isLeader: cfg.leader,
			pl:       pl,
			grpcPort: cfg.grpcPort,
			wavPath:  cfg.wavPath,
			offsetCh: offsetCh,
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

			for range ticker.C {
				state, err := a.roomClient.RoomState()
				if err != nil {
					log.Printf("room state lookup failed: %v", err)
					continue
				}

				log.Printf("room state: leader=%s peers=%d", state.Leader, len(state.Peers))
				a.pl.Reset()
				for _, p := range state.Peers {
					role := models.Role(p.Role)
					if p.ID == a.cfg.id {
						continue
					}
					a.pl.Add(p.ID, p.Address, role, p.ControlPort)
					if role == models.RoleLeader {
						log.Printf("leader is %s at %s", p.ID, p.Address)
					}
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

		for range ticker.C {
			leader := a.pl.Leader()
			if !shouldProbeLeader(a.cfg, leader) {
				if leader == nil {
					log.Printf("follower %s has not discovered a leader yet", a.cfg.id)
				}
				continue
			}

			log.Printf("follower %s discovered leader %s at %s", a.cfg.id, leader.ID, leader.Address)
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

package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"silent/internal/discovery"
	"silent/internal/models"
	"silent/internal/peerlist"
	"silent/internal/sync"
)

func main() {
	id := flag.String("id", "peer-1", "node identifier")
	broadcastIP := flag.String("broadcast-ip", "255.255.255.255", "broadcast address for peer discovery/scheduling")
	port := flag.Int("port", 9999, "UDP discovery port")
	controlPortFlag := flag.Int("control-port", 0, "UDP control port")
	leader := flag.Bool("leader", false, "act as the leader for scheduling")
	wavPath := flag.String("wav", "", "optional wav file to play")
	flag.Parse()

	broadcastAddr, err := validateBroadcastIP(*broadcastIP)
	if err != nil {
		log.Fatalf("invalid broadcast IP: %v", err)
	}

	pl := peerlist.New()
	ann := discovery.NewAnnouncer(discovery.Config{
		ID:          *id,
		Port:        *port,
		Leader:      *leader,
		BroadcastIP: broadcastAddr,
	})
	ann.SetSeenCallback(func(p models.Peer) {
		host := p.Address
		if h, _, err := net.SplitHostPort(p.Address); err == nil {
			host = h
		}
		pl.Add(p.ID, host, p.Role)
	})

	controlPort := *controlPortFlag
	if controlPort == 0 {
		controlPort = *port + 1
	}
	listener, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: controlPort})
	if err != nil {
		log.Fatalf("listen on control port: %v", err)
	}
	defer listener.Close()

	offsetCh := make(chan time.Duration, 1)
	go func() {
		for {
			if err := ann.Announce(); err != nil {
				log.Printf("announce failed: %v", err)
			}
			time.Sleep(1 * time.Second)
		}
	}()

	go func() {
		if err := ann.Start(); err != nil {
			log.Printf("discovery stopped: %v", err)
		}
	}()

	go func() {
		if err := handleControl(listener, *leader, *id, *wavPath, offsetCh); err != nil {
			log.Printf("control loop stopped: %v", err)
		}
	}()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)

	fmt.Printf("peer %s listening on udp:%d (control:%d)\n", *id, *port, controlPort)
	if *leader {
		fmt.Println("leader mode enabled")
		go func() {
			time.Sleep(2 * time.Second)
			shared := time.Now().Add(3 * time.Second)
			if err := broadcastSchedule(controlPort, *id, shared, *wavPath, broadcastAddr); err != nil {
				log.Printf("broadcast failed: %v", err)
			}
			fmt.Printf("leader broadcast shared playback at %s\n", shared.Format(time.RFC3339Nano))
		}()
	} else {
		go func() {
			time.Sleep(1 * time.Second)
			if leader := pl.Leader(); leader != nil {
				if err := probeLeader(*leader, *id, controlPort, offsetCh); err != nil {
					log.Printf("clock sync failed: %v", err)
				}
			}
		}()
	}

	<-sigCh
	ann.Stop()
	fmt.Println("shutting down")
}

func handleControl(listener *net.UDPConn, leader bool, id, wavPath string, offsetCh chan time.Duration) error {
	buf := make([]byte, 1024)
	var offset time.Duration
	for {
		n, addr, err := listener.ReadFromUDP(buf)
		recv := time.Now()
		if err != nil {
			return err
		}
		msg := strings.TrimSpace(string(buf[:n]))
		parts := strings.Split(msg, "|")
		if len(parts) == 0 {
			continue
		}
		switch parts[0] {
		case "SYNC":
			if !leader {
				continue
			}
			if len(parts) < 2 {
				continue
			}
			_, err := strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				continue
			}
			serverSend := time.Now()
			response := fmt.Sprintf("SYNC-ACK|%d|%d", recv.UnixNano(), serverSend.UnixNano())
			_, _ = listener.WriteToUDP([]byte(response), addr)
			fmt.Printf("leader received sync ping from %s\n", addr.String())
		case "PLAY":
			if leader || len(parts) < 3 {
				continue
			}
			sharedAtNanos, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				continue
			}
			sharedAt := time.Unix(0, sharedAtNanos)
			select {
			case currentOffset := <-offsetCh:
				offset = currentOffset
			default:
			}
			localAt := sync.ConvertSharedTimeToLocal(sharedAt, offset)
			fmt.Printf("scheduled playback for %s (local %s)\n", sharedAt.Format(time.RFC3339Nano), localAt.Format(time.RFC3339Nano))
			go schedulePlayback(localAt, wavPath)
		}
	}
}

func probeLeader(peer models.Peer, id string, controlPort int, offsetCh chan time.Duration) error {
	addr, err := parsePeerAddress(peer.Address, controlPort)
	if err != nil {
		return err
	}
	conn, err := net.Dial("udp4", addr)
	if err != nil {
		return err
	}
	defer conn.Close()
	clientSend := time.Now().UnixNano()
	_, err = conn.Write([]byte("SYNC|" + strconv.FormatInt(clientSend, 10)))
	if err != nil {
		return err
	}
	buf := make([]byte, 1024)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, err := conn.Read(buf)
	if err != nil {
		return err
	}
	clientRecv := time.Now()
	parts := strings.Split(strings.TrimSpace(string(buf[:n])), "|")
	if len(parts) < 3 {
		return fmt.Errorf("invalid sync response: %q", string(buf[:n]))
	}
	serverRecvNanos, _ := strconv.ParseInt(parts[1], 10, 64)
	serverSendNanos, _ := strconv.ParseInt(parts[2], 10, 64)
	serverRecv := time.Unix(0, serverRecvNanos)
	serverSend := time.Unix(0, serverSendNanos)
	offset := sync.ComputeOffset(time.Unix(0, clientSend), serverRecv, serverSend, clientRecv)
	select {
	case offsetCh <- offset:
	default:
	}
	fmt.Printf("%s synced with leader, offset=%s\n", id, offset)
	return nil
}

func parsePeerAddress(peer string, controlPort int) (string, error) {
	host := strings.TrimSpace(peer)

	if strings.Contains(host, "@") {
		parts := strings.SplitN(host, "@", 2)
		if len(parts) != 2 || strings.TrimSpace(parts[1]) == "" {
			return "", fmt.Errorf("invalid peer address %q", peer)
		}
		host = strings.TrimSpace(parts[1])
	}

	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}

	if host == "" {
		return "", fmt.Errorf("invalid peer address %q", peer)
	}

	return net.JoinHostPort(host, strconv.Itoa(controlPort)), nil
}

func validateBroadcastIP(raw string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", fmt.Errorf("broadcast IP cannot be empty")
	}

	ip := net.ParseIP(raw)
	if ip == nil {
		return "", fmt.Errorf("invalid broadcast IP %q: must be a valid IP address", raw)
	}

	if ip.To4() == nil {
		return "", fmt.Errorf("invalid broadcast IP %q: IPv6 is not supported here", raw)
	}

	if ip.IsLoopback() || ip.IsUnspecified() {
		return "", fmt.Errorf("invalid broadcast IP %q: use a real broadcast address such as 192.168.1.255", raw)
	}

	if strings.Count(raw, ".") != 3 {
		return "", fmt.Errorf("invalid broadcast IP %q: expected an IPv4 address", raw)
	}

	return ip.String(), nil
}

func broadcastSchedule(controlPort int, id string, shared time.Time, wavPath, broadcastAddr string) error {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.ParseIP(broadcastAddr), Port: controlPort})
	if err != nil {
		return err
	}
	defer conn.Close()

	// if err := discovery.EnableBroadcast(conn); err != nil {
	// 	return err
	// }

	msg := fmt.Sprintf("PLAY|%s|%d", id, shared.UnixNano())
	log.Printf("main: sending PLAY to %s:%d -> %s", broadcastAddr, controlPort, msg)

	_, err = conn.Write([]byte(msg))
	return err
}

func schedulePlayback(at time.Time, wavPath string) {
	delay := time.Until(at)
	if delay > 0 {
		time.Sleep(delay)
	}
	if wavPath != "" {
		if _, err := exec.LookPath("aplay"); err == nil {
			cmd := exec.Command("aplay", wavPath)
			if err := cmd.Start(); err != nil {
				log.Printf("playback failed: %v", err)
			}
			return
		}
		if _, err := exec.LookPath("ffplay"); err == nil {
			cmd := exec.Command("ffplay", "-nodisp", "-autoexit", wavPath)
			if err := cmd.Start(); err != nil {
				log.Printf("playback failed: %v", err)
			}
			return
		}
		log.Printf("no audio player available for %s", wavPath)
		return
	}
	fmt.Print("\a")
}

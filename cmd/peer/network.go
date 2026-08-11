package main

import (
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"silent/internal/discovery"
	"silent/internal/models"
	"silent/internal/sync"
)

func handleControl(listener *net.UDPConn, leader bool) error {
	buf := make([]byte, 1024)

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
			if _, err := strconv.ParseInt(parts[1], 10, 64); err != nil {
				continue
			}

			serverSend := time.Now()
			response := fmt.Sprintf("SYNC-ACK|%d|%d", recv.UnixNano(), serverSend.UnixNano())
			_, _ = listener.WriteToUDP([]byte(response), addr)
			log.Printf("leader received sync ping from %s\n", addr.String())
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
	if _, err := conn.Write([]byte("SYNC|" + strconv.FormatInt(clientSend, 10))); err != nil {
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

	offset := sync.ComputeOffset(
		time.Unix(0, clientSend),
		serverRecv,
		serverSend,
		clientRecv,
	)

	select {
	case offsetCh <- offset:
	default:
	}

	log.Printf("%s synced with leader, offset=%s\n", id, offset)
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

func advertiseAddress(grpcPort int, override string) string {
	if override != "" {
		return net.JoinHostPort(override, strconv.Itoa(grpcPort))
	}

	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, addr := range addrs {
			if ipnet, ok := addr.(*net.IPNet); ok && !ipnet.IP.IsLoopback() && ipnet.IP.To4() != nil {
				return net.JoinHostPort(ipnet.IP.String(), strconv.Itoa(grpcPort))
			}
		}
	}

	return net.JoinHostPort("127.0.0.1", strconv.Itoa(grpcPort))
}

func registerWithRoom(cfg config, roomURL string) *discovery.Client {
	role := "FOLLOWER"
	if cfg.leader {
		role = "LEADER"
	}

	peer := discovery.PeerInfo{
		ID:          cfg.id,
		RoomID:      "demo-room",
		Address:     advertiseAddress(cfg.grpcPort, cfg.advertiseHost),
		Role:        role,
		ControlPort: effectiveControlPort(cfg),
	}

	client := discovery.NewClient(roomURL, peer)

	go func() {
		for {
			if err := client.Register(10 * time.Second); err != nil {
				log.Printf("room register failed: %v", err)
				time.Sleep(2 * time.Second)
				continue
			}
			time.Sleep(5 * time.Second)
		}
	}()

	return client
}

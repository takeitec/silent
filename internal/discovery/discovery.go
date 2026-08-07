package discovery

import (
	"fmt"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"silent/internal/models"
)

// Announcer periodically broadcasts the presence of a peer and listens for other peers on the network.
type Announcer struct {
	ID          string
	Port        int
	StopCh      chan struct{}
	mu          sync.Mutex
	peers       map[string]models.Peer
	seen        func(models.Peer)
	Leader      bool
	BroadcastIP string
}

type Config struct {
	ID          string
	Port        int
	Leader      bool
	BroadcastIP string
}

func NewAnnouncer(cfg Config) *Announcer {
	return &Announcer{
		ID:          cfg.ID,
		Port:        cfg.Port,
		Leader:      cfg.Leader,
		BroadcastIP: cfg.BroadcastIP,
		StopCh:      make(chan struct{}),
		peers:       make(map[string]models.Peer),
	}
}

func (a *Announcer) SetSeenCallback(fn func(models.Peer)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = fn
}

func EnableBroadcast(conn *net.UDPConn) error {
	var sockErr error

	rawConn, err := conn.SyscallConn()
	if err != nil {
		return fmt.Errorf("syscall conn: %w", err)
	}

	err = rawConn.Control(func(fd uintptr) {
		sockErr = setBroadcast(fd)
	})
	if err != nil {
		return fmt.Errorf("control socket: %w", err)
	}
	if sockErr != nil {
		return fmt.Errorf("set socket option: %w", sockErr)
	}
	return nil
}

func (a *Announcer) Start() error {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: a.Port})
	if err != nil {
		return err
	}
	defer conn.Close()

	log.Printf("discovery: listening for UDP on 0.0.0.0:%d", a.Port)

	buf := make([]byte, 1024)
	for {
		select {
		case <-a.StopCh:
			return nil
		default:
		}
		conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil {
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				continue
			}
			return err
		}

		log.Printf("discovery: received packet from %s (%d bytes): %q", addr.String(), n, string(buf[:n]))

		parts := strings.SplitN(string(buf[:n]), "|", 2)
		peer := models.Peer{ID: parts[0], Address: addr.String(), SeenAt: time.Now()}
		if len(parts) > 1 {
			peer.Role = models.Role(parts[1])
		}
		if len(peer.ID) == 0 {
			peer.ID = addr.String()
		}

		a.mu.Lock()
		a.peers[peer.Address] = peer
		if a.seen != nil {
			a.seen(peer)
		}
		a.mu.Unlock()
	}
}

func (a *Announcer) Announce() error {
	target := net.IPv4bcast
	if a.BroadcastIP != "" {
		if parsed := net.ParseIP(a.BroadcastIP); parsed != nil {
			target = parsed
		} else {
			log.Printf("discovery: invalid broadcast IP %q, using default %s", a.BroadcastIP, net.IPv4bcast.String())
		}
	}

	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: target, Port: a.Port})
	if err != nil {
		return err
	}
	defer conn.Close()

	// if err := EnableBroadcast(conn); err != nil {
	// 	return fmt.Errorf("enable broadcast: %w", err)
	// }

	role := models.RoleFollower
	if a.Leader {
		role = models.RoleLeader
	}
	msg := []byte(fmt.Sprintf("%s|%s", a.ID, role))
	log.Printf("discovery: sending announce from %q as %s to %s:%d", a.ID, role, target.String(), a.Port)

	_, err = conn.Write(msg)
	if err != nil {
		return fmt.Errorf("write udp: %w", err)
	}
	return nil
}

func (a *Announcer) Peers() []models.Peer {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]models.Peer, 0, len(a.peers))
	for _, peer := range a.peers {
		out = append(out, peer)
	}
	return out
}

func (a *Announcer) Stop() {
	select {
	case <-a.StopCh:
	default:
		close(a.StopCh)
	}
}

func ExamplePeer() {
	fmt.Println("peer")
}

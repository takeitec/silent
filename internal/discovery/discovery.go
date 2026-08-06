package discovery

import (
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"silent/internal/models"
)

// Announcer periodically broadcasts the presence of a peer and listens for other peers on the network.
type Announcer struct {
	ID     string
	Port   int
	StopCh chan struct{}
	mu     sync.Mutex
	peers  map[string]models.Peer
	seen   func(models.Peer)
}

func NewAnnouncer(id string, port int) *Announcer {
	return &Announcer{ID: id, Port: port, StopCh: make(chan struct{}), peers: make(map[string]models.Peer)}
}

func (a *Announcer) SetSeenCallback(fn func(models.Peer)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.seen = fn
}

func (a *Announcer) Start() error {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.ParseIP("0.0.0.0"), Port: a.Port})
	if err != nil {
		return err
	}
	defer conn.Close()

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

func (a *Announcer) Announce(leader bool) error {
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4bcast, Port: a.Port})
	if err != nil {
		return err
	}
	defer conn.Close()

	role := models.RoleFollower
	if leader {
		role = models.RoleLeader
	}
	msg := []byte(fmt.Sprintf("%s|%s", a.ID, role))
	_, err = conn.Write(msg)
	return err
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

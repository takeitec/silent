package discovery

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// Peer describes an advertised device on the LAN.
type Peer struct {
	ID      string
	Address string
	SeenAt  time.Time
}

// Announcer periodically broadcasts presence on the LAN.
type Announcer struct {
	ID     string
	Port   int
	StopCh chan struct{}
	mu     sync.Mutex
	peers  map[string]Peer
	seen   func(Peer)
}

func NewAnnouncer(id string, port int) *Announcer {
	return &Announcer{ID: id, Port: port, StopCh: make(chan struct{}), peers: make(map[string]Peer)}
}

func (a *Announcer) SetSeenCallback(fn func(Peer)) {
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
		peer := Peer{ID: string(buf[:n]), Address: addr.String(), SeenAt: time.Now()}
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
	conn, err := net.DialUDP("udp4", nil, &net.UDPAddr{IP: net.IPv4bcast, Port: a.Port})
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := []byte(a.ID)
	_, err = conn.Write(msg)
	return err
}

func (a *Announcer) Peers() []Peer {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Peer, 0, len(a.peers))
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

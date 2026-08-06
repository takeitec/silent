package peerlist

import (
	"silent/internal/models"
	"sync"
)

// PeerList tracks discovered peers for the current room.
type PeerList struct {
	mu    sync.Mutex
	peers map[string]models.Peer
}

func New() *PeerList {
	return &PeerList{peers: make(map[string]models.Peer)}
}

func (p *PeerList) Add(id, addr string, role models.Role) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers[id] = models.Peer{ID: id, Address: addr, Role: role}
}

func (p *PeerList) Peers() []models.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]models.Peer, 0, len(p.peers))
	for _, peer := range p.peers {
		out = append(out, peer)
	}
	return out
}

func (p *PeerList) Leader() *models.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, peer := range p.peers {
		if peer.Role == models.RoleLeader {
			return &peer
		}
	}
	return nil
}

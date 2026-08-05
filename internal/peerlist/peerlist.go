package peerlist

import "sync"

// PeerList tracks discovered peers for the current room.
type PeerList struct {
	mu    sync.Mutex
	peers map[string]string
}

func New() *PeerList {
	return &PeerList{peers: make(map[string]string)}
}

func (p *PeerList) Add(id, addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers[id] = addr
}

func (p *PeerList) Peers() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, 0, len(p.peers))
	for id, addr := range p.peers {
		out = append(out, id+"@"+addr)
	}
	return out
}

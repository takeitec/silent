package main

import (
	"encoding/json"
	"log"
	"net"
	"net/http"
	"silent/internal/discovery"
	"sync"
	"time"
)

type Server struct {
	mu       sync.RWMutex
	roomID   string
	peers    map[string]discovery.PeerInfo
	expires  map[string]time.Time
	listen   string
	stopCh   chan struct{}
	interval time.Duration
}

func NewServer(roomID, listen string) *Server {
	return &Server{
		roomID:   roomID,
		peers:    make(map[string]discovery.PeerInfo),
		expires:  make(map[string]time.Time),
		listen:   listen,
		stopCh:   make(chan struct{}),
		interval: 2 * time.Second,
	}
}

func (s *Server) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/register", s.handleRegister)
	mux.HandleFunc("/room", s.handleRoom)

	ln, err := net.Listen("tcp", s.listen)
	if err != nil {
		return err
	}

	log.Printf("room server: listening on %s", s.listen)
	go s.cleanupLoop()
	go func() {
		if err := http.Serve(ln, mux); err != nil {
			log.Printf("room server: http serve stopped: %v", err)
		}
	}()

	return nil
}

func (s *Server) Stop() {
	close(s.stopCh)
}

func (s *Server) handleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req discovery.RegisterRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	log.Printf("room server: register request peer=%s role=%s room=%s", req.Peer.ID, req.Peer.Role, req.Peer.RoomID)

	s.mu.Lock()
	defer s.mu.Unlock()

	if req.Peer.RoomID == "" {
		req.Peer.RoomID = s.roomID
	}

	ttl := time.Duration(req.TTLSeconds) * time.Second
	if ttl <= 0 {
		ttl = 10 * time.Second
	}

	s.peers[req.Peer.ID] = req.Peer
	s.expires[req.Peer.ID] = time.Now().Add(ttl)
	log.Printf("room server: registered peer=%s ttl=%s", req.Peer.ID, ttl)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"ok": true,
	})
}

func (s *Server) handleRoom(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state := discovery.RoomState{
		RoomID: s.roomID,
		Peers:  make([]discovery.PeerInfo, 0, len(s.peers)),
	}

	for _, p := range s.peers {
		state.Peers = append(state.Peers, p)
	}

	log.Printf("room server: room snapshot room=%s peers=%d leader=%s", s.roomID, len(state.Peers), state.Leader)

	for _, p := range state.Peers {
		if p.Role == "LEADER" {
			state.Leader = p.ID
			break
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(state)
}

func (s *Server) cleanupLoop() {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.mu.Lock()
			now := time.Now()
			removed := 0
			for id, exp := range s.expires {
				if now.After(exp) {
					delete(s.peers, id)
					delete(s.expires, id)
					removed++
				}
			}
			s.mu.Unlock()
			if removed > 0 {
				log.Printf("room server: cleaned up %d expired peer(s)", removed)
			}
		case <-s.stopCh:
			return
		}
	}
}

func main() {
	server := NewServer("demo-room", "0.0.0.0:9100")
	if err := server.Start(); err != nil {
		panic(err)
	}

	select {}
}

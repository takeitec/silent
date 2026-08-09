package discovery

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type PeerInfo struct {
	ID          string `json:"id"`
	RoomID      string `json:"room_id"`
	Address     string `json:"address"`
	Role        string `json:"role"`
	ControlPort int    `json:"control_port"`
}

type RegisterRequest struct {
	Peer       PeerInfo `json:"peer"`
	TTLSeconds int      `json:"ttl_seconds"`
}

type RoomState struct {
	RoomID string     `json:"room_id"`
	Peers  []PeerInfo `json:"peers"`
	Leader string     `json:"leader"`
}

type Client struct {
	baseURL string
	peer    PeerInfo
}

func NewClient(baseURL string, peer PeerInfo) *Client {
	return &Client{
		baseURL: baseURL,
		peer:    peer,
	}
}

func (c *Client) Register(ttl time.Duration) error {
	reqBody := RegisterRequest{
		Peer:       c.peer,
		TTLSeconds: int(ttl.Seconds()),
	}

	body, err := json.Marshal(reqBody)
	if err != nil {
		return err
	}

	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Post(c.baseURL+"/register", "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("register failed: %s", resp.Status)
	}

	return nil
}

func (c *Client) RoomState() (RoomState, error) {
	resp, err := http.Get(c.baseURL + "/room")
	if err != nil {
		return RoomState{}, err
	}
	defer resp.Body.Close()

	var state RoomState
	if err := json.NewDecoder(resp.Body).Decode(&state); err != nil {
		return RoomState{}, err
	}

	return state, nil
}

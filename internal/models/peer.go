package models

import "time"

// Role represents the role of a peer in the network.
type Role string

const (
	RoleLeader   Role = "LEADER"
	RoleFollower Role = "FOLLOWER"
)

// Peer describes an individual peer in the network.
type Peer struct {
	ID          string
	Address     string
	Role        Role
	ControlPort int
	SeenAt      time.Time
}

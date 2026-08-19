package main

import (
	"context"
	"fmt"
	stdsync "sync"
	"time"
)

// sessionState is the explicit lifecycle of a stream session on this peer,
// whether it's acting as leader or follower for that session. Previously
// this was implicit: a session was "active" if present in one map with an
// unexpired lease, and "cancellable" if present in a second, separate map -
// two loosely-coupled maps that had to be kept in sync by convention, with
// no record of *why* a session ended or whether the last attempt actually
// succeeded.
type sessionState int

const (
	sessionIdle sessionState = iota
	sessionStarting
	sessionActive
	sessionReconnecting
	sessionDraining
	sessionFailed
	sessionEnded
)

func (st sessionState) String() string {
	switch st {
	case sessionIdle:
		return "idle"
	case sessionStarting:
		return "starting"
	case sessionActive:
		return "active"
	case sessionReconnecting:
		return "reconnecting"
	case sessionDraining:
		return "draining"
	case sessionFailed:
		return "failed"
	case sessionEnded:
		return "ended"
	default:
		return "unknown"
	}
}

type sessionRole int

const (
	roleUnknown sessionRole = iota
	roleLeader
	roleFollower
)

func (r sessionRole) String() string {
	switch r {
	case roleLeader:
		return "leader"
	case roleFollower:
		return "follower"
	default:
		return "unknown"
	}
}

// validSessionTransitions is the explicit state machine. Any transition not
// listed here is rejected (logged and ignored) rather than silently
// applied - this is what actually enforces the machine, as opposed to it
// being documentation that the code doesn't check.
var validSessionTransitions = map[sessionState]map[sessionState]bool{
	sessionIdle:         {sessionStarting: true},
	sessionStarting:     {sessionActive: true, sessionFailed: true, sessionEnded: true},
	sessionActive:       {sessionReconnecting: true, sessionDraining: true, sessionFailed: true, sessionEnded: true},
	sessionReconnecting: {sessionActive: true, sessionFailed: true, sessionEnded: true},
	sessionDraining:     {sessionEnded: true},
	// Failed/Ended are terminal for a given attempt, but a fresh explicit
	// restart (a brand new StartStreamPlayback for the same session ID)
	// is allowed to begin again from either.
	sessionFailed: {sessionStarting: true},
	sessionEnded:  {sessionStarting: true},
}

// sessionRecord is the single source of truth for one session ID on this
// peer. Where the old code had activeSessions[id] (a lease expiry) and
// sessionCancels[id] (a cancel func) as two maps that both had to be kept
// in sync, this is one record with one state field.
type sessionRecord struct {
	id          string
	role        sessionRole
	state       sessionState
	cancel      context.CancelFunc
	startedAt   time.Time
	updatedAt   time.Time
	leaseExpiry time.Time
	attempt     int // reconnect attempt count for the current run; reset on reaching sessionActive
	lastErr     error
}

type sessionManager struct {
	mu       stdsync.Mutex
	sessions map[string]*sessionRecord
	lease    time.Duration
}

func newSessionManager(lease time.Duration) *sessionManager {
	if lease <= 0 {
		lease = 30 * time.Second
	}
	return &sessionManager{sessions: make(map[string]*sessionRecord), lease: lease}
}

// Begin starts a brand-new attempt for a session ID. Returns false if one
// is already in a non-terminal state (Starting/Active/Reconnecting/
// Draining) - this is the single duplicate-start guard, replacing the two
// separate checks (sessionCancels presence, then activeSessions expiry)
// the old beginSession had, which could disagree with each other.
func (m *sessionManager) Begin(id string, role sessionRole) (*sessionRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id = normaliseSessionID(id)
	now := time.Now()

	if rec, ok := m.sessions[id]; ok {
		switch rec.state {
		case sessionStarting, sessionActive, sessionReconnecting, sessionDraining:
			// Always reject while something is still nominally running -
			// no auto-heal here. If a session really is stuck, that's
			// Sweep's job to detect and clean up in the background, not
			// something this call should decide unilaterally on the
			// caller's behalf. The caller gets a clear "already in
			// progress" instead, and can retry once Sweep has cleared it
			// (or after an explicit StopStreamPlayback).
			return nil, false
		}
	}

	rec := &sessionRecord{
		id:          id,
		role:        role,
		state:       sessionStarting,
		startedAt:   now,
		updatedAt:   now,
		leaseExpiry: now.Add(m.lease),
	}
	m.sessions[id] = rec
	logInfof("session state: transition session=%q role=%s from=idle to=starting", id, role)
	return rec, true
}

// Transition moves a session to a new state, enforcing the transition
// table and refreshing the lease/attempt bookkeeping. err is recorded as
// the reason and surfaces in logs and in Get() for anything that wants to
// report why a session ended.
func (m *sessionManager) Transition(id string, to sessionState, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	id = normaliseSessionID(id)
	rec, ok := m.sessions[id]
	if !ok {
		logWarnf("session state: transition for unknown session=%q to=%s (ignored)", id, to)
		return
	}

	from := rec.state
	if allowed := validSessionTransitions[from]; !allowed[to] {
		logWarnf("session state: rejected invalid transition session=%q from=%s to=%s err=%v", id, from, to, err)
		return
	}

	rec.state = to
	rec.updatedAt = time.Now()
	rec.lastErr = err

	switch to {
	case sessionActive:
		rec.attempt = 0
		rec.leaseExpiry = rec.updatedAt.Add(m.lease)
	case sessionReconnecting:
		rec.attempt++
		rec.leaseExpiry = rec.updatedAt.Add(m.lease)
	}

	logInfof("session state: transition session=%q role=%s from=%s to=%s attempt=%d err=%v", id, rec.role, from, to, rec.attempt, err)
}

// Heartbeat refreshes the lease without changing state, so a long-lived
// Active session doesn't get swept just for running longer than the lease
// duration. Call this periodically from whatever loop owns the session
// (e.g. once per health-log interval).
func (m *sessionManager) Heartbeat(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id = normaliseSessionID(id)
	if rec, ok := m.sessions[id]; ok {
		rec.leaseExpiry = time.Now().Add(m.lease)
	}
}

func (m *sessionManager) SetCancel(id string, cancel context.CancelFunc) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id = normaliseSessionID(id)
	if rec, ok := m.sessions[id]; ok {
		rec.cancel = cancel
	}
}

func (m *sessionManager) ClearCancel(id string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	id = normaliseSessionID(id)
	if rec, ok := m.sessions[id]; ok {
		rec.cancel = nil
	}
}

// Cancel invokes the session's cancel func if one is registered and the
// session isn't already terminal. Returns false if there's nothing to
// cancel (already ended/failed, or never started) - matches the old
// cancelSession's return semantics so callers don't need to change.
func (m *sessionManager) Cancel(id string) bool {
	m.mu.Lock()
	id = normaliseSessionID(id)
	rec, ok := m.sessions[id]
	m.mu.Unlock()

	if !ok || rec.cancel == nil {
		logInfof("session state: cancel requested for non-active session=%q", id)
		return false
	}
	switch rec.state {
	case sessionEnded, sessionFailed, sessionIdle:
		return false
	}
	logInfof("session state: canceling active session=%q", id)
	rec.cancel()
	return true
}

func (m *sessionManager) Get(id string) (*sessionRecord, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	rec, ok := m.sessions[normaliseSessionID(id)]
	return rec, ok
}

// IsRunning reports whether a session is in a non-terminal state. This is
// what most call sites actually want to know ("is something already
// happening for this ID"), as opposed to the raw state value.
func (m *sessionManager) IsRunning(id string) bool {
	rec, ok := m.Get(id)
	if !ok {
		return false
	}
	switch rec.state {
	case sessionStarting, sessionActive, sessionReconnecting, sessionDraining:
		return true
	default:
		return false
	}
}

// Sweep clears old terminal sessions (so the map doesn't grow forever
// across a long-lived room) and force-fails any session whose lease
// expired without a Heartbeat/Transition call - which means the goroutine
// that owned it died (panicked, was killed, deadlocked) without going
// through normal cleanup. Call this periodically, e.g. from a ticker in
// main.go.
func (m *sessionManager) Sweep(now time.Time, retainTerminal time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for id, rec := range m.sessions {
		switch rec.state {
		case sessionEnded, sessionFailed:
			if now.Sub(rec.updatedAt) > retainTerminal {
				delete(m.sessions, id)
			}
		default:
			if now.After(rec.leaseExpiry) {
				logWarnf("session state: lease expired without heartbeat session=%q state=%s - canceling and marking failed", id, rec.state)
				// If the owner is genuinely dead this is a no-op,
				// but if it was only slow to heartbeat (e.g. stuck on an
				// initial connect that hasn't failed yet), this is what
				// actually stops it, rather than just relabeling the
				// record while the real goroutine keeps running
				// unaffected.
				if rec.cancel != nil {
					rec.cancel()
					rec.cancel = nil
				}
				rec.state = sessionFailed
				rec.updatedAt = now
				rec.lastErr = fmt.Errorf("lease expired without heartbeat")
			}
		}
	}
}

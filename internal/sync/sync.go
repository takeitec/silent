package sync

import "time"

// ComputeOffset derives a simple clock offset using an NTP-style exchange.
// It estimates the difference between the server clock and the client clock.
func ComputeOffset(clientSend, serverRecv, serverSend, clientRecv time.Time) time.Duration {
	return (serverRecv.Sub(clientSend) + serverSend.Sub(clientRecv)) / 2
}

// ConvertSharedTimeToLocal converts a shared-clock timestamp into the local clock.
func ConvertSharedTimeToLocal(sharedTime time.Time, offset time.Duration) time.Time {
	return sharedTime.Add(-offset)
}

// NextPlaybackTime returns the local time at which to schedule playback for a shared deadline.
func NextPlaybackTime(sharedDeadline time.Time, lead time.Duration) time.Time {
	return sharedDeadline.Add(lead)
}

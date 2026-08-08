package main

import "testing"

func TestParsePeerAddress(t *testing.T) {
	got, err := parsePeerAddress("peer@192.168.0.10:9999", 50051)
	if err != nil {
		t.Fatalf("parsePeerAddress returned error: %v", err)
	}

	want := "192.168.0.10:50051"
	if got != want {
		t.Fatalf("parsePeerAddress() = %q, want %q", got, want)
	}
}

func TestValidateBroadcastIP(t *testing.T) {
	got, err := validateBroadcastIP("192.168.1.255")
	if err != nil {
		t.Fatalf("validateBroadcastIP returned error: %v", err)
	}

	if got != "192.168.1.255" {
		t.Fatalf("validateBroadcastIP() = %q, want %q", got, "192.168.1.255")
	}
}

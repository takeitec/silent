package main

import (
	"flag"
	"os"
	"testing"
)

func TestParseFlagsRoomDefaultsToTrue(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	os.Args = []string{"peer"}

	cfg := parseFlags()
	if !cfg.room {
		t.Fatalf("expected room mode to default to true")
	}
}

func TestParseFlagsRoomCanBeDisabled(t *testing.T) {
	oldArgs := os.Args
	oldCommandLine := flag.CommandLine
	defer func() {
		os.Args = oldArgs
		flag.CommandLine = oldCommandLine
	}()

	flag.CommandLine = flag.NewFlagSet(os.Args[0], flag.ContinueOnError)
	os.Args = []string{"peer", "-room=false"}

	cfg := parseFlags()
	if cfg.room {
		t.Fatalf("expected room mode to be disabled when flag is false")
	}
}

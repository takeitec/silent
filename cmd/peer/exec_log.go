package main

import (
	"log"
	"os/exec"
)

var mediaCommandLoggingEnabled = true

func setMediaCommandLoggingEnabled(enabled bool) {
	mediaCommandLoggingEnabled = enabled
}

func logExecStart(kind string, cmd *exec.Cmd) {
	if !mediaCommandLoggingEnabled {
		return
	}
	if cmd == nil {
		return
	}
	log.Printf("exec: starting %s command: %s", kind, cmd.String())
}

func logExecError(kind string, cmd *exec.Cmd, stage string, err error) {
	if !mediaCommandLoggingEnabled {
		return
	}
	if err == nil {
		return
	}
	if cmd != nil {
		log.Printf("exec: %s command failed during %s: %v (%s)", kind, stage, err, cmd.String())
		return
	}
	log.Printf("exec: %s command failed during %s: %v", kind, stage, err)
}

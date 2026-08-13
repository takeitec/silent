package main

import (
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
	logInfof("exec: starting %s command: %s", kind, cmd.String())
}

func logExecError(kind string, cmd *exec.Cmd, stage string, err error) {
	if !mediaCommandLoggingEnabled {
		return
	}
	if err == nil {
		return
	}
	if cmd != nil {
		logWarnf("exec: %s command failed during %s: %v (%s)", kind, stage, err, cmd.String())
		return
	}
	logWarnf("exec: %s command failed during %s: %v", kind, stage, err)
}

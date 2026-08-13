package main

import "go.uber.org/zap"

func logDebugf(format string, args ...interface{}) {
	zap.S().Debugf(format, args...)
}

func logInfof(format string, args ...interface{}) {
	zap.S().Infof(format, args...)
}

func logWarnf(format string, args ...interface{}) {
	zap.S().Warnf(format, args...)
}

func logErrorf(format string, args ...interface{}) {
	zap.S().Errorf(format, args...)
}

func logFatalf(format string, args ...interface{}) {
	zap.S().Fatalf(format, args...)
}

func logInfo(msg string) {
	zap.S().Info(msg)
}

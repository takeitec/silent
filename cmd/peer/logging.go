package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	logOutputStdout = "stdout"
	logOutputFile   = "file"
	logOutputBoth   = "both"

	logTimeRFC3339Nano = "rfc3339nano"
	logTimeRFC3339     = "rfc3339"
)

func normaliseLogOutput(mode string) string {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case logOutputStdout, logOutputFile, logOutputBoth:
		return strings.ToLower(strings.TrimSpace(mode))
	default:
		return logOutputBoth
	}
}

func normaliseLogTimeFormat(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case logTimeRFC3339:
		return time.RFC3339
	case logTimeRFC3339Nano:
		return time.RFC3339Nano
	default:
		return time.RFC3339Nano
	}
}

func normaliseLogLevel(value string) zapcore.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "debug":
		return zap.DebugLevel
	case "info":
		return zap.InfoLevel
	case "warn", "warning":
		return zap.WarnLevel
	case "error":
		return zap.ErrorLevel
	default:
		return zap.DebugLevel
	}
}

func appendSessionSuffix(fileName, suffix string) string {
	if strings.TrimSpace(fileName) == "" || strings.TrimSpace(suffix) == "" {
		return fileName
	}

	ext := filepath.Ext(fileName)
	base := strings.TrimSuffix(fileName, ext)
	if base == "" {
		base = "log"
	}

	return fmt.Sprintf("%s-%s%s", base, suffix, ext)
}

func configureAppLogging(cfg config) (func(), error) {
	outputMode := normaliseLogOutput(cfg.logOutput)
	timeFormat := normaliseLogTimeFormat(cfg.logTimeFormat)
	logLevel := normaliseLogLevel(cfg.logLevel)

	outputPaths := make([]string, 0, 2)
	if outputMode == logOutputStdout || outputMode == logOutputBoth {
		outputPaths = append(outputPaths, "stdout")
	}

	if outputMode == logOutputFile || outputMode == logOutputBoth {
		logDir := strings.TrimSpace(cfg.logDir)
		if logDir == "" {
			logDir = "logs"
		}
		if err := os.MkdirAll(logDir, 0o755); err != nil {
			return nil, fmt.Errorf("create log directory %q: %w", logDir, err)
		}

		fileName := strings.TrimSpace(cfg.logFileName)
		if fileName == "" {
			fileName = fmt.Sprintf("silent-peer-%s.log", sanitizeForFilename(cfg.id))
		}
		fileName = appendSessionSuffix(fileName, cfg.logSessionStamp)

		logPath := filepath.Join(logDir, fileName)
		outputPaths = append(outputPaths, logPath)
	}

	if len(outputPaths) == 0 {
		outputPaths = append(outputPaths, "stdout")
	}

	zapCfg := zap.Config{
		Level:       zap.NewAtomicLevelAt(logLevel),
		Development: false,
		Encoding:    "console",
		EncoderConfig: zapcore.EncoderConfig{
			MessageKey:     "msg",
			LevelKey:       "level",
			TimeKey:        "ts",
			NameKey:        "logger",
			CallerKey:      "caller",
			StacktraceKey:  "stacktrace",
			LineEnding:     zapcore.DefaultLineEnding,
			EncodeLevel:    zapcore.LowercaseLevelEncoder,
			EncodeTime:     zapcore.TimeEncoderOfLayout(timeFormat),
			EncodeDuration: zapcore.StringDurationEncoder,
			EncodeCaller:   zapcore.ShortCallerEncoder,
		},
		OutputPaths:       outputPaths,
		ErrorOutputPaths:  outputPaths,
		DisableCaller:     true,
		DisableStacktrace: true,
	}

	logger, err := zapCfg.Build()
	if err != nil {
		return nil, fmt.Errorf("build zap logger: %w", err)
	}

	undoStdLog := zap.RedirectStdLog(logger)
	zap.ReplaceGlobals(logger)

	cleanup := func() {
		undoStdLog()
		_ = logger.Sync()
	}

	if outputMode == logOutputFile || outputMode == logOutputBoth {
		if len(outputPaths) > 0 {
			logInfof("app log: output=%s level=%s file=%s", outputMode, logLevel.String(), outputPaths[len(outputPaths)-1])
		}
	} else {
		logInfof("app log: output=%s level=%s", outputMode, logLevel.String())
	}

	return cleanup, nil
}

package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var logger *zap.Logger

func SetUpLogger(isDebug bool) error {
	var level zapcore.Level

	if isDebug {
		level = zap.DebugLevel
	} else {
		level = zap.InfoLevel
	}

	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = "@timestamp"
	encoderConfig.MessageKey = "message"
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

	core := zapcore.NewCore(
		zapcore.NewJSONEncoder(encoderConfig),
		zapcore.AddSync(os.Stdout),
		level,
	)

	logger = zap.New(
		core,
		zap.AddCaller(),
		zap.AddStacktrace(zap.ErrorLevel),
	)

	return nil
}

func GetLogger() *zap.Logger {
	return logger
}

// Sync flushes any buffered log entries; call it on shutdown.
func Sync() error {
	if logger == nil {
		return nil
	}
	return logger.Sync()
}

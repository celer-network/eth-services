package logger

import (
	"fmt"

	"github.com/celer-network/eth-services/types"
	"go.uber.org/zap"
)

type ZapLogger struct {
	*zap.SugaredLogger
}

var _ types.Logger = (*ZapLogger)(nil)

// NewZapLogger creates a wrapped zap logger
func NewZapLogger(logger *zap.SugaredLogger) *ZapLogger {
	return &ZapLogger{
		SugaredLogger: logger,
	}
}

// Trace is a shim stand-in for when we have real trace-level logging support
func (zl *ZapLogger) Trace(args ...interface{}) {
	zl.Debug(append([]interface{}{"TRACE: "}, args...))
}

// Tracef is a shim stand-in for when we have real trace-level logging support
func (zl *ZapLogger) Tracef(format string, values ...interface{}) {
	zl.Debugf("TRACE: " + fmt.Sprintf(format, values...))
}

// Tracew is a shim stand-in for when we have real trace-level logging support
func (zl *ZapLogger) Tracew(msg string, keysAndValues ...interface{}) {
	zl.Debugw("TRACE: "+msg, keysAndValues...)
}

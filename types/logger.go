package types

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Panic(args ...interface{})
	Error(args ...interface{})
	Warn(args ...interface{})
	Info(args ...interface{})
	Debug(args ...interface{})
	Trace(args ...interface{})

	Panicw(msg string, keysAndValues ...interface{})
	Errorw(msg string, keysAndValues ...interface{})
	Warnw(msg string, keysAndValues ...interface{})
	Infow(msg string, keysAndValues ...interface{})
	Debugw(msg string, keysAndValues ...interface{})
	Tracew(msg string, keysAndValues ...interface{})

	Panicf(format string, v ...interface{})
	Errorf(format string, v ...interface{})
	Warnf(format string, v ...interface{})
	Infof(format string, v ...interface{})
	Debugf(format string, v ...interface{})
	Tracef(format string, v ...interface{})
}

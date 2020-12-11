package types

// Logger is implemented by any logging system that is used for standard logs.
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	Tracef(string, ...interface{})
}

// Errorf logs an ERROR log message to the logger specified in opts or to the
// global logger if no logger is specified in opts.
func (opt *Options) Errorf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Errorf(format, v...)
}

// Infof logs an INFO message to the logger specified in opts.
func (opt *Options) Infof(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Infof(format, v...)
}

// Warningf logs a WARNING message to the logger specified in opts.
func (opt *Options) Warningf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Warningf(format, v...)
}

// Debugf logs a DEBUG message to the logger specified in opts.
func (opt *Options) Debugf(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Debugf(format, v...)
}

// Tracef logs a TRACE message to the logger specified in opts.
func (opt *Options) Tracef(format string, v ...interface{}) {
	if opt.Logger == nil {
		return
	}
	opt.Logger.Tracef(format, v...)
}

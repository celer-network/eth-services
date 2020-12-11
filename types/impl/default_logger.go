package impl

import (
	"log"
	"os"
)

type loggingLevel int

const (
	TraceLevel loggingLevel = iota
	DebugLevel
	InfoLevel
	WarningLevel
	ErrorLevel
)

type defaultLog struct {
	*log.Logger
	level loggingLevel
}

func defaultLogger(level loggingLevel) *defaultLog {
	return &defaultLog{Logger: log.New(os.Stderr, "eth-services ", log.LstdFlags), level: level}
}

func (l *defaultLog) Errorf(f string, v ...interface{}) {
	if l.level <= ErrorLevel {
		l.Printf("ERROR: "+f, v...)
	}
}

func (l *defaultLog) Warningf(f string, v ...interface{}) {
	if l.level <= WarningLevel {
		l.Printf("WARNING: "+f, v...)
	}
}

func (l *defaultLog) Infof(f string, v ...interface{}) {
	if l.level <= InfoLevel {
		l.Printf("INFO: "+f, v...)
	}
}

func (l *defaultLog) Debugf(f string, v ...interface{}) {
	if l.level <= DebugLevel {
		l.Printf("DEBUG: "+f, v...)
	}
}

func (l *defaultLog) Tracef(f string, v ...interface{}) {
	if l.level <= TraceLevel {
		l.Printf("TRACE: "+f, v...)
	}
}

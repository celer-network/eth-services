package impl

import (
	"github.com/rs/zerolog"
)

type Logger struct {
	log zerolog.Logger
}

func NewLogger(logger zerolog.Logger) *Logger {
	return &Logger{
		log: logger.With().Str("component", "eth-services").Logger(),
	}
}

func (l *Logger) Errorf(msg string, args ...interface{}) {
	l.log.Error().Msgf(msg, args...)
}

func (l *Logger) Warningf(msg string, args ...interface{}) {
	l.log.Warn().Msgf(msg, args...)
}

func (l *Logger) Infof(msg string, args ...interface{}) {
	l.log.Info().Msgf(msg, args...)
}

func (l *Logger) Debugf(msg string, args ...interface{}) {
	l.log.Debug().Msgf(msg, args...)
}

func (l *Logger) Tracef(msg string, args ...interface{}) {
	l.log.Trace().Msgf(msg, args...)
}

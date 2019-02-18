package testutil

import (
	"testing"

	"github.com/go-kit/kit/log"
)

type logger struct {
	t *testing.T
}

// NewLogger returns a gokit compatible Logger which calls t.Log.
func NewLogger(t *testing.T) log.Logger {
	return logger{t: t}
}

// Log implements log.Logger.
func (t logger) Log(keyvals ...interface{}) error {
	t.t.Log(keyvals...)
	return nil
}

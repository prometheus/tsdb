package testutil

import (
	"testing"

	"github.com/go-kit/kit/log"
)

type logger struct {
	t *testing.T
}

func NewLogger(t *testing.T) log.Logger {
	return logger{t: t}
}

func (t logger) Log(keyvals ...interface{}) error {
	t.t.Log(keyvals...)
	return nil
}

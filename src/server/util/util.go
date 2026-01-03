/*
Package util contains utility functions for other packages
*/
package util

import "log/slog"

func SlogPanic(s string, args ...any) {
	slog.Error(s, args...)
	panic(s)
}

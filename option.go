package boltutil

import (
	"os"
	"time"

	"go.etcd.io/bbolt"
)

type innerOption struct {
	FileMode os.FileMode
	Options  *bbolt.Options
}

// Options represents the options that can be set when opening a database.
type Option func(options *innerOption)

// WithFileMode return Option with specified FileMode
func WithFileMode(fileMode os.FileMode) Option {
	return func(options *innerOption) {
		options.FileMode = fileMode
	}
}

// WithTimeout return Option with specified Timeout
func WithTimeout(timeout time.Duration) Option {
	return func(options *innerOption) {
		options.Options.Timeout = timeout
	}
}

// WithNoGrowSync return Option with specified NoGrowSync
func WithNoGrowSync(noGrowSync bool) Option {
	return func(options *innerOption) {
		options.Options.NoGrowSync = noGrowSync
	}
}

// WithNoFreelistSync return Option with specified NoFreelistSync
func WithNoFreelistSync(noFreelistSync bool) Option {
	return func(options *innerOption) {
		options.Options.NoFreelistSync = noFreelistSync
	}
}

// WithFreelistType return Option with specified FreelistType
func WithFreelistType(freelistType bbolt.FreelistType) Option {
	return func(options *innerOption) {
		options.Options.FreelistType = freelistType
	}
}

// WithReadOnly return Option with specified ReadOnly
func WithReadOnly(readOnly bool) Option {
	return func(options *innerOption) {
		options.Options.ReadOnly = readOnly
	}
}

// WithMmapFlags return Option with specified MmapFlags
func WithMmapFlags(mmapFlags int) Option {
	return func(options *innerOption) {
		options.Options.MmapFlags = mmapFlags
	}
}

// WithInitialMmapSize return Option with specified InitialMmapSize
func WithInitialMmapSize(initialMmapSize int) Option {
	return func(options *innerOption) {
		options.Options.InitialMmapSize = initialMmapSize
	}
}

// WithPageSize return Option with specified PageSize
func WithPageSize(pageSize int) Option {
	return func(options *innerOption) {
		options.Options.PageSize = pageSize
	}
}

// WithNoSync return Option with specified NoSync
func WithNoSync(noSync bool) Option {
	return func(options *innerOption) {
		options.Options.NoSync = noSync
	}
}

// WithOpenFile return Option with specified OpenFile,
func WithOpenFile(openFile func(string, int, os.FileMode) (*os.File, error)) Option {
	return func(options *innerOption) {
		options.Options.OpenFile = openFile
	}
}

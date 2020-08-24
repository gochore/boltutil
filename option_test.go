package boltutil

import (
	"os"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func TestWithOption(t *testing.T) {
	want := &innerOption{
		FileMode: 0600,
		Options: &bbolt.Options{
			Timeout:         time.Second,
			NoGrowSync:      true,
			NoFreelistSync:  true,
			FreelistType:    bbolt.FreelistArrayType,
			ReadOnly:        true,
			MmapFlags:       1,
			InitialMmapSize: 1,
			PageSize:        1,
			NoSync:          true,
			OpenFile:        nil,
		},
	}

	options := []Option{
		WithFileMode(want.FileMode),
		WithTimeout(want.Options.Timeout),
		WithNoGrowSync(want.Options.NoGrowSync),
		WithNoFreelistSync(want.Options.NoFreelistSync),
		WithFreelistType(want.Options.FreelistType),
		WithReadOnly(want.Options.ReadOnly),
		WithMmapFlags(want.Options.MmapFlags),
		WithInitialMmapSize(want.Options.InitialMmapSize),
		WithPageSize(want.Options.PageSize),
		WithNoSync(want.Options.NoSync),
		WithOpenFile(want.Options.OpenFile),
	}

	got := &innerOption{
		Options: &bbolt.Options{
			OpenFile: os.OpenFile,
		},
	}
	for _, v := range options {
		v(got)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

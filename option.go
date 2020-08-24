package boltutil

import (
	"os"

	"go.etcd.io/bbolt"
)

type innerOption struct {
	FileMode os.FileMode
	Options  *bbolt.Options
}

type Option func(options *innerOption)

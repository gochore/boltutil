package boltutil

// Storable is the interface that can be stored into bolt.
type Storable interface {
	HasBucket
	Key() []byte
}

// HasBucket is the interface that indicates the bound bucket
type HasBucket interface {
	Bucket() []byte
}

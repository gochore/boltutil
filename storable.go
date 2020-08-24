package boltutil

type Storable interface {
	HasBucket
	Key() []byte
}

type HasBucket interface {
	Bucket() []byte
}

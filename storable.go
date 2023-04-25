package boltutil

// Storable is the interface that can be stored into bolt.
type Storable interface {
	HasBucket
	BoltKey() []byte
}

// HasBucket is the interface that indicates the bound bucket
type HasBucket interface {
	BoltBucket() []byte
}

// HasCoder is the interface that indicates the Coder of the type
type HasCoder interface {
	BoltCoder() Coder
}

// HasBeforePut is the interface that indicates the BeforePut method
type HasBeforePut interface {
	BeforePut(id uint64) // will be called before put, id is an auto incrementing integer for the bucket
}

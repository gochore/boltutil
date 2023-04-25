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

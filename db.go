package boltutil

import (
	"bytes"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

type DB struct {
	db           *bbolt.DB
	defaultCoder Coder
}

// Open creates and opens a database with given options.
func Open(path string, options ...Option) (*DB, error) {
	option := &innerOption{
		FileMode:     0600,
		DefaultCoder: GobCoder{},
		Options: func() *bbolt.Options {
			v := *bbolt.DefaultOptions
			return &v
		}(),
	}
	for _, v := range options {
		v(option)
	}

	db, err := bbolt.Open(path, option.FileMode, option.Options)
	if err != nil {
		return nil, err
	}

	return &DB{
		db:           db,
		defaultCoder: option.DefaultCoder,
	}, nil
}

// Wrap return a DB with then given bbolt.DB
func Wrap(db *bbolt.DB) *DB {
	return &DB{
		db: db,
	}
}

// Unwrap return the original bbolt.DB
func (d *DB) Unwrap() *bbolt.DB {
	return d.db
}

// Get injects storable objects with their keys.
func (d *DB) Get(objs ...Storable) error {
	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range objs {
		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			return ErrNotFound
		}
		got := bucket.Get(obj.BoltKey())
		if got == nil {
			return ErrNotFound
		}
		if err := d.getCoder(obj).Decode(bytes.NewBuffer(got), obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, obj.BoltKey(), err)
		}
	}

	return nil
}

// GetAll injects all storable objects in the bucket.
func (d *DB) GetAll(result interface{}) error {
	if reflect.TypeOf(result).Kind() != reflect.Ptr {
		return fmt.Errorf("should be slice pointer: %T", result)
	}

	slice := reflect.ValueOf(result).Elem()
	if slice.Kind() != reflect.Slice {
		return fmt.Errorf("should be slice pointer: %T", result)
	}

	if slice.Len() != 0 {
		return fmt.Errorf("should be empty: len %d", slice.Len())
	}

	itemType := slice.Type().Elem()
	if itemType.Kind() != reflect.Ptr {
		return fmt.Errorf("item should be pointer: %v", itemType)
	}
	itemType = itemType.Elem()
	item := reflect.New(itemType).Interface()

	var bucketName []byte
	var coder Coder
	if obj, ok := item.(Storable); ok {
		bucketName = obj.BoltBucket()
		coder = d.getCoder(obj)
	} else {
		return fmt.Errorf("item should implement Storable: %T", item)
	}

	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(bucketName)
	if bucket == nil {
		return nil
	}

	cur := bucket.Cursor()
	for k, v := cur.First(); k != nil; k, v = cur.Next() {
		obj := reflect.New(itemType).Interface()
		if err := coder.Decode(bytes.NewBuffer(v), obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, k, err)
		}
		slice.Set(reflect.Append(slice, reflect.ValueOf(obj)))
	}

	return nil
}

// Count return count of kv in the bucket.
func (d *DB) Count(hasBucket HasBucket) (int, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)

	bucket := tx.Bucket(hasBucket.BoltBucket())
	if bucket == nil {
		return 0, nil
	}

	ret := 0
	cur := bucket.Cursor()
	for k, _ := cur.First(); k != nil; k, _ = cur.Next() {
		ret++
	}
	return ret, nil
}

// Put store storables into database, create bucket if does not exists.
func (d *DB) Put(storables ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range storables {
		buffer := &bytes.Buffer{}
		if err := d.getCoder(obj).Encode(buffer, obj); err != nil {
			return fmt.Errorf("encode %T %q: %w", obj, obj.BoltKey(), err)
		}

		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			if bucket, err = tx.CreateBucketIfNotExists(obj.BoltBucket()); err != nil {
				return err
			}
		}
		if err := bucket.Put(obj.BoltKey(), buffer.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// Delete remove values by key of storables
func (d *DB) Delete(storables ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range storables {
		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			continue
		}
		if err := bucket.Delete(obj.BoltKey()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteBucket remove the specified buckets
func (d *DB) DeleteBucket(hasBuckets ...HasBucket) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range hasBuckets {
		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			continue
		}
		if err := tx.DeleteBucket(obj.BoltBucket()); err != nil {
			return nil
		}
	}
	return tx.Commit()
}

// DeleteAllBucket remove all buckets
func (d *DB) DeleteAllBucket() error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	var buckets [][]byte
	if err := tx.ForEach(func(name []byte, _ *bbolt.Bucket) error {
		buckets = append(buckets, name)
		return nil
	}); err != nil {
		return err
	}

	for _, bucket := range buckets {
		if err := tx.DeleteBucket(bucket); err != nil {
			return nil
		}
	}
	return tx.Commit()
}

func rollback(tx *bbolt.Tx) {
	_ = tx.Rollback()
}

func (d *DB) getCoder(obj interface{}) Coder {
	if v, ok := obj.(HasCoder); ok {
		return v.BoltCoder()
	}
	return d.defaultCoder
}

package boltutil

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	"go.etcd.io/bbolt"
)

type DB struct {
	db *bbolt.DB
}

//  Open creates and opens a database with given options.
func Open(path string, options ...Option) (*DB, error) {
	option := &innerOption{
		FileMode: 0600,
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
		db: db,
	}, nil
}

//  Wrap return a DB with then given bbolt.DB
func Wrap(db *bbolt.DB) *DB {
	return &DB{
		db: db,
	}
}

//  Unwrap return the original bbolt.DB
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
		bucket := tx.Bucket(obj.Bucket())
		if bucket == nil {
			return ErrNotFound
		}
		got := bucket.Get(obj.Key())
		if got == nil {
			return ErrNotFound
		}
		dec := gob.NewDecoder(bytes.NewBuffer(got))
		if err := dec.Decode(obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, obj.Key(), err)
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
	if obj, ok := item.(Storable); ok {
		bucketName = obj.Bucket()
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
		dec := gob.NewDecoder(bytes.NewBuffer(v))
		if err := dec.Decode(obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, k, err)
		}
		slice.Set(reflect.Append(slice, reflect.ValueOf(obj)))
	}

	return nil
}

// GetAll injects count of keys in the bucket.
func (d *DB) Count(hasBucket HasBucket, count interface{}) error {
	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(hasBucket.Bucket())

	errNilCount := fmt.Errorf("nil count")

	switch c := count.(type) {
	case *uint8:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *uint16:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *uint32:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *uint64:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *int8:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *int16:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *int32:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *int64:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *float32:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *float64:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *int:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	case *uint:
		if c == nil {
			return errNilCount
		}
		*c = 0
		if bucket != nil {
			return bucket.ForEach(func(_, _ []byte) error {
				*c++
				return nil
			})
		}
	default:
		return fmt.Errorf("should be number pointer: %T", count)
	}

	return nil
}

func (d *DB) Put(storables ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range storables {
		buffer := &bytes.Buffer{}
		enc := gob.NewEncoder(buffer)
		if err := enc.Encode(obj); err != nil {
			return fmt.Errorf("encode %T %q: %w", obj, obj.Key(), err)
		}

		bucket := tx.Bucket(obj.Bucket())
		if bucket == nil {
			if bucket, err = tx.CreateBucketIfNotExists(obj.Bucket()); err != nil {
				return err
			}
		}
		if err := bucket.Put(obj.Key(), buffer.Bytes()); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func (d *DB) Delete(storables ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range storables {
		bucket := tx.Bucket(obj.Bucket())
		if bucket == nil {
			continue
		}
		if err := bucket.Delete(obj.Key()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (d *DB) Flush(hasBuckets ...HasBucket) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range hasBuckets {
		bucket := tx.Bucket(obj.Bucket())
		if bucket == nil {
			continue
		}
		if err := tx.DeleteBucket(obj.Bucket()); err != nil {
			return nil
		}
	}
	return tx.Commit()
}

func (d *DB) FlushAll() error {
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

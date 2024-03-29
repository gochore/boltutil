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

// Close closes the database.
func (d *DB) Close() error {
	return d.db.Close()
}

// Get injects storable object with its key.
func (d *DB) Get(obj Storable, conditions ...*Condition) error {
	var condition *Condition
	if len(conditions) == 1 {
		condition = conditions[0]
	} else if len(conditions) > 1 {
		return fmt.Errorf("too many conditions")
	}

	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		if condition.getIgnoreIfNotExist() {
			return nil
		}
		return ErrNotExist
	}
	got := bucket.Get(obj.BoltKey())
	if got == nil {
		if condition.getIgnoreIfNotExist() {
			return nil
		}
		return ErrNotExist
	}
	if err := d.getCoder(obj).Decode(bytes.NewReader(got), obj); err != nil {
		return fmt.Errorf("decode %T %q: %w", obj, obj.BoltKey(), err)
	}

	return nil
}

// Put stores storable object.
func (d *DB) Put(obj Storable, conditions ...*Condition) error {
	var condition *Condition
	if len(conditions) == 1 {
		condition = conditions[0]
	} else if len(conditions) > 1 {
		return fmt.Errorf("too many conditions")
	}

	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		if condition.getFailIfNotExist() {
			return ErrNotExist
		}
		if bucket, err = tx.CreateBucketIfNotExists(obj.BoltBucket()); err != nil {
			return err
		}
	}

	if condition.getIgnoreIfExist() || condition.getFailIfExist() || condition.getFailIfNotExist() {
		got := bucket.Get(obj.BoltKey())
		if got != nil {
			if condition.getIgnoreIfExist() {
				return nil
			}
			if condition.getFailIfExist() {
				return ErrAlreadyExist
			}
		} else if condition.getFailIfNotExist() {
			return ErrNotExist
		}
	}

	if v, ok := obj.(HasBeforePut); ok {
		id, err := bucket.NextSequence()
		if err != nil {
			return err
		}
		v.BeforePut(id)
	}

	buffer := &bytes.Buffer{}
	if err := d.getCoder(obj).Encode(buffer, obj); err != nil {
		return fmt.Errorf("encode %T %q: %w", obj, obj.BoltKey(), err)
	}

	if err := bucket.Put(obj.BoltKey(), buffer.Bytes()); err != nil {
		return err
	}

	return tx.Commit()
}

// Delete deletes storable object.
func (d *DB) Delete(obj Storable, conditions ...*Condition) error {
	var condition *Condition
	if len(conditions) == 1 {
		condition = conditions[0]
	} else if len(conditions) > 1 {
		return fmt.Errorf("too many conditions")
	}

	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		if condition.getFailIfNotExist() {
			return ErrNotExist
		}
		return nil
	}
	if err := bucket.Delete(obj.BoltKey()); err != nil {
		return err
	}
	return tx.Commit()
}

// MGet injects storable objects with their keys.
func (d *DB) MGet(objs ...Storable) error {
	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range objs {
		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			return ErrNotExist
		}
		got := bucket.Get(obj.BoltKey())
		if got == nil {
			return ErrNotExist
		}
		if err := d.getCoder(obj).Decode(bytes.NewReader(got), obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, obj.BoltKey(), err)
		}
	}

	return nil
}

// MPut store storables into database, create bucket if it does not exist.
func (d *DB) MPut(objs ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	buffer := &bytes.Buffer{}
	for _, obj := range objs {
		bucket := tx.Bucket(obj.BoltBucket())
		if bucket == nil {
			if bucket, err = tx.CreateBucketIfNotExists(obj.BoltBucket()); err != nil {
				return err
			}
		}

		if v, ok := obj.(HasBeforePut); ok {
			id, err := bucket.NextSequence()
			if err != nil {
				return err
			}
			v.BeforePut(id)
		}

		buffer.Reset()
		if err := d.getCoder(obj).Encode(buffer, obj); err != nil {
			return fmt.Errorf("encode %T %q: %w", obj, obj.BoltKey(), err)
		}

		value := make([]byte, buffer.Len())
		copy(value, buffer.Bytes())
		if err := bucket.Put(obj.BoltKey(), value); err != nil {
			return err
		}
	}

	return tx.Commit()
}

// MDelete remove values by key of storables
func (d *DB) MDelete(objs ...Storable) error {
	tx, err := d.db.Begin(true)
	if err != nil {
		return err
	}
	defer rollback(tx)

	for _, obj := range objs {
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

// Scan scans values in the bucket and put them into result.
func (d *DB) Scan(result any, filters ...*Filter) error {
	var filter *Filter
	if len(filters) == 1 {
		filter = filters[0]
	} else if len(filters) > 1 {
		return fmt.Errorf("too many filters")
	}

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
	if seek := filter.seek(); seek != nil {
		cur.Seek(seek)
	}
SCAN:
	for k, v := cur.First(); filter.goon(k); k, v = cur.Next() {
		for _, c := range filter.getConditions() {
			if c == nil {
				continue
			}
			skip, stop := c(k, v)
			if stop {
				break SCAN
			}
			if skip {
				continue SCAN
			}
		}

		obj := reflect.New(itemType).Interface()
		if err := coder.Decode(bytes.NewReader(v), obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, k, err)
		}

		for _, c := range filter.getStorableConditions() {
			if c == nil {
				continue
			}
			skip, stop := c(obj.(Storable))
			if stop {
				break SCAN
			}
			if skip {
				continue SCAN
			}
		}

		slice.Set(reflect.Append(slice, reflect.ValueOf(obj)))
	}

	return nil
}

// First injects the first value in the bucket into result.
func (d *DB) First(obj Storable, filters ...*Filter) error {
	var filter *Filter
	if len(filters) == 1 {
		filter = filters[0]
	} else if len(filters) > 1 {
		return fmt.Errorf("too many filters")
	}

	tx, err := d.db.Begin(false)
	if err != nil {
		return err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		return nil
	}

	cur := bucket.Cursor()
	if seek := filter.seek(); seek != nil {
		cur.Seek(seek)
	}
SCAN:
	for k, v := cur.First(); filter.goon(k); k, v = cur.Next() {
		for _, c := range filter.getConditions() {
			if c == nil {
				continue
			}
			skip, stop := c(k, v)
			if stop {
				break SCAN
			}
			if skip {
				continue SCAN
			}
		}
		if err := d.getCoder(obj).Decode(bytes.NewReader(v), obj); err != nil {
			return fmt.Errorf("decode %T %q: %w", obj, k, err)
		}
		for _, c := range filter.getStorableConditions() {
			if c == nil {
				continue
			}
			skip, stop := c(obj)
			if stop {
				break SCAN
			}
			if skip {
				continue SCAN
			}
		}
		return nil
	}
	return ErrNotExist
}

// Count return count of kv in the bucket.
func (d *DB) Count(obj Storable, filters ...*Filter) (int, error) {
	var filter *Filter
	if len(filters) == 1 {
		filter = filters[0]
	} else if len(filters) > 1 {
		return 0, fmt.Errorf("too many filters")
	}

	tx, err := d.db.Begin(false)
	if err != nil {
		return 0, err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		return 0, nil
	}

	count := 0
	cur := bucket.Cursor()
	if seek := filter.seek(); seek != nil {
		cur.Seek(seek)
	}
SCAN:
	for k, v := cur.First(); filter.goon(k); k, v = cur.Next() {
		for _, c := range filter.getConditions() {
			if c == nil {
				continue
			}
			skip, stop := c(k, v)
			if stop {
				break SCAN
			}
			if skip {
				continue SCAN
			}
		}
		if len(filter.getStorableConditions()) > 0 {
			if err := d.getCoder(obj).Decode(bytes.NewReader(v), obj); err != nil {
				return 0, fmt.Errorf("decode %T %q: %w", obj, k, err)
			}
			for _, c := range filter.getStorableConditions() {
				if c == nil {
					continue
				}
				skip, stop := c(obj)
				if stop {
					break SCAN
				}
				if skip {
					continue SCAN
				}
			}
		}
		count++
	}
	return count, nil
}

// Exist check if the storable exist
func (d *DB) Exist(obj Storable) (bool, error) {
	tx, err := d.db.Begin(false)
	if err != nil {
		return false, err
	}
	defer rollback(tx)

	bucket := tx.Bucket(obj.BoltBucket())
	if bucket == nil {
		return false, nil
	}
	got := bucket.Get(obj.BoltKey())
	return got != nil, nil
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

func (d *DB) getCoder(obj any) Coder {
	if v, ok := obj.(HasCoder); ok {
		return v.BoltCoder()
	}
	return d.defaultCoder
}

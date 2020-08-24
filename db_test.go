package boltutil

import (
	"fmt"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"go.etcd.io/bbolt"
)

func testDB(t *testing.T, empty ...bool) *DB {
	db, _ := Open(filepath.Join(t.TempDir(), "bolt.db"))
	if len(empty) > 0 && empty[0] {
		return db
	}
	_ = db.Put(
		&Person{
			Id:   "jason",
			Name: "Jason Song",
			Age:  25,
		},
		&Person{
			Id:   "vivia",
			Name: "Vivia Lei",
			Age:  25,
		},
		&Car{
			Id:        1,
			Name:      "tesla",
			CreatedAt: time.Now(),
		},
	)

	boom := &Car{
		Id:        0,
		Name:      "boom",
		CreatedAt: time.Now(),
	}
	_ = db.Unwrap().Update(func(tx *bbolt.Tx) error {
		return tx.Bucket(boom.Bucket()).Put(boom.Key(), []byte("can not decode"))
	})
	return db
}

func TestWrap(t *testing.T) {
	db := &bbolt.DB{}
	type args struct {
		db *bbolt.DB
	}
	tests := []struct {
		name string
		args args
		want *DB
	}{
		{
			name: "regular",
			args: args{
				db: db,
			},
			want: &DB{
				db: db,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := Wrap(tt.args.db); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Wrap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_Unwrap(t *testing.T) {
	db := &bbolt.DB{}

	type fields struct {
		db *bbolt.DB
	}
	tests := []struct {
		name   string
		fields fields
		want   *bbolt.DB
	}{
		{
			name: "regular",
			fields: fields{
				db: db,
			},
			want: db,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &DB{
				db: tt.fields.db,
			}
			if got := d.Unwrap(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Unwrap() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestOpen(t *testing.T) {
	type args struct {
		path    string
		options []Option
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				path:    filepath.Join(t.TempDir(), "bolt.db"),
				options: nil,
			},
			wantErr: false,
		},
		{
			name: "with options",
			args: args{
				path:    filepath.Join(t.TempDir(), "bolt.db"),
				options: []Option{WithTimeout(time.Second)},
			},
			wantErr: false,
		},
		{
			name: "invalid path",
			args: args{
				path:    filepath.Join(t.TempDir(), "invalid", "bolt.db"),
				options: []Option{WithTimeout(time.Second)},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := Open(tt.args.path, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("Open() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				if err = got.Unwrap().Sync(); err != nil {
					t.Errorf("invalid db: %v", err)
					return
				}
			}
		})
	}
}

func TestDB_Get(t *testing.T) {
	type args struct {
		objs []Storable
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				objs: []Storable{
					&Person{Id: "jason"},
				},
			},
			wantErr: false,
		},
		{
			name: "key not exists",
			args: args{
				objs: []Storable{
					&Person{Id: "trump"},
				},
			},
			wantErr: true,
		},
		{
			name: "bucket not exists",
			args: args{
				objs: []Storable{
					&Wind{},
				},
			},
			wantErr: true,
		},
		{
			name: "can not decode",
			args: args{
				objs: []Storable{
					&Car{Id: 0},
				},
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).Get(tt.args.objs...); (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_GetAll(t *testing.T) {
	type args struct {
		result interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				result: func() interface{} {
					var ret []*Person
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "not pointer",
			args: args{
				result: make([]*Person, 0),
			},
			wantErr: true,
		},
		{
			name: "not slice",
			args: args{
				result: &Person{},
			},
			wantErr: true,
		},
		{
			name: "not empty",
			args: args{
				result: func() interface{} {
					ret := make([]*Person, 1)
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "wrong item",
			args: args{
				result: func() interface{} {
					var ret []Person
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "not storable",
			args: args{
				result: func() interface{} {
					var ret []*string
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "bucket not exists",
			args: args{
				result: func() interface{} {
					var ret []*Wind
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "can not decode",
			args: args{
				result: func() interface{} {
					var ret []*Car
					return &ret
				}(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).GetAll(tt.args.result); (err != nil) != tt.wantErr {
				t.Errorf("GetAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Count(t *testing.T) {
	type args struct {
		hasBucket HasBucket
		count     interface{}
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{

		{
			name: "uint8",
			args: args{
				hasBucket: &Person{},
				count: func() *uint8 {
					var ret uint8
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "uint16",
			args: args{
				hasBucket: &Person{},
				count: func() *uint16 {
					var ret uint16
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "uint32",
			args: args{
				hasBucket: &Person{},
				count: func() *uint32 {
					var ret uint32
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "uint64",
			args: args{
				hasBucket: &Person{},
				count: func() *uint64 {
					var ret uint64
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "int8",
			args: args{
				hasBucket: &Person{},
				count: func() *int8 {
					var ret int8
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "int16",
			args: args{
				hasBucket: &Person{},
				count: func() *int16 {
					var ret int16
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "int32",
			args: args{
				hasBucket: &Person{},
				count: func() *int32 {
					var ret int32
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "int64",
			args: args{
				hasBucket: &Person{},
				count: func() *int64 {
					var ret int64
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "float32",
			args: args{
				hasBucket: &Person{},
				count: func() *float32 {
					var ret float32
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "float64",
			args: args{
				hasBucket: &Person{},
				count: func() *float64 {
					var ret float64
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "int",
			args: args{
				hasBucket: &Person{},
				count: func() *int {
					var ret int
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "uint",
			args: args{
				hasBucket: &Person{},
				count: func() *uint {
					var ret uint
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "nil",
			args: args{
				hasBucket: &Person{},
				count:     nil,
			},
			wantErr: true,
		},
		{
			name: "string",
			args: args{
				hasBucket: &Person{},
				count:     "1",
			},
			wantErr: true,
		},
		{
			name: "nil int",
			args: args{
				hasBucket: &Person{},
				count:     (*int)(nil),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).Count(tt.args.hasBucket, tt.args.count); (err != nil) != tt.wantErr {
				t.Errorf("Count() error = %v, wantErr %v", err, tt.wantErr)
			} else if err == nil {
				if count := fmt.Sprintf("%v", reflect.ValueOf(tt.args.count).Elem().Interface()); count != "2" {
					t.Errorf("Count() got %+v, want %v", count, 2)
				}
			}
		})
	}
}

func TestDB_Put(t *testing.T) {
	type args struct {
		storables []Storable
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				storables: []Storable{
					&Person{
						Id:   "hei",
						Name: "Xiao Hei",
						Age:  2,
					},
					&Car{
						Id:        2,
						Name:      "Ya Di",
						CreatedAt: time.Now(),
					},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t, true).Put(tt.args.storables...); (err != nil) != tt.wantErr {
				t.Errorf("Put() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Delete(t *testing.T) {
	type args struct {
		storables []Storable
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				storables: []Storable{
					&Person{
						Id: "jason",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "bucket not exists",
			args: args{
				storables: []Storable{
					&Wind{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).Delete(tt.args.storables...); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Flush(t *testing.T) {
	type args struct {
		hasBuckets []HasBucket
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				hasBuckets: []HasBucket{
					&Person{
						Id: "jason",
					},
				},
			},
			wantErr: false,
		},
		{
			name: "bucket not exists",
			args: args{
				hasBuckets: []HasBucket{
					&Wind{},
				},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).Flush(tt.args.hasBuckets...); (err != nil) != tt.wantErr {
				t.Errorf("Flush() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_FlushAll(t *testing.T) {
	tests := []struct {
		name    string
		wantErr bool
	}{
		{
			name:    "regular",
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).FlushAll(); (err != nil) != tt.wantErr {
				t.Errorf("FlushAll() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

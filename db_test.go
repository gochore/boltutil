package boltutil

import (
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/bbolt"
)

func testDB(t *testing.T, empty ...bool) *DB {
	db, _ := Open(filepath.Join(t.TempDir(), "bolt.db"))
	if len(empty) > 0 && empty[0] {
		return db
	}
	_ = db.MPut(
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
		return tx.Bucket(boom.BoltBucket()).Put(boom.BoltKey(), []byte("dirty test data, can not be decoded"))
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
			if err := testDB(t).MGet(tt.args.objs...); (err != nil) != tt.wantErr {
				t.Errorf("MGet() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Scan(t *testing.T) {
	type args struct {
		result any
		cond   *Filter
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				result: func() any {
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
				result: func() any {
					ret := make([]*Person, 1)
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "wrong item",
			args: args{
				result: func() any {
					var ret []Person
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "not storable",
			args: args{
				result: func() any {
					var ret []*string
					return &ret
				}(),
			},
			wantErr: true,
		},
		{
			name: "bucket not exists",
			args: args{
				result: func() any {
					var ret []*Wind
					return &ret
				}(),
			},
			wantErr: false,
		},
		{
			name: "can not decode",
			args: args{
				result: func() any {
					var ret []*Car
					return &ret
				}(),
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := testDB(t).Scan(tt.args.result, tt.args.cond); (err != nil) != tt.wantErr {
				t.Errorf("Scan() error = %v, wantErr %v", err, tt.wantErr)
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
			if err := testDB(t, true).MPut(tt.args.storables...); (err != nil) != tt.wantErr {
				t.Errorf("MPut() error = %v, wantErr %v", err, tt.wantErr)
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
			if err := testDB(t).MDelete(tt.args.storables...); (err != nil) != tt.wantErr {
				t.Errorf("MDelete() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_DeleteBucket(t *testing.T) {
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
			if err := testDB(t).DeleteBucket(tt.args.hasBuckets...); (err != nil) != tt.wantErr {
				t.Errorf("DeleteBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_DeleteAllBucket(t *testing.T) {
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
			if err := testDB(t).DeleteAllBucket(); (err != nil) != tt.wantErr {
				t.Errorf("DeleteAllBucket() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDB_Count(t *testing.T) {
	type args struct {
		obj Storable
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{
			name: "regular",
			args: args{
				obj: &Person{},
			},
			want:    2,
			wantErr: false,
		},
		{
			name: "bucket not exists",
			args: args{
				obj: &Wind{},
			},
			want:    0,
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := testDB(t).Count(tt.args.obj, nil)
			if (err != nil) != tt.wantErr {
				t.Errorf("Count() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Count() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDB_BeforePut(t *testing.T) {
	t.Run("set id", func(t *testing.T) {
		db := testDB(t, true)
		require.NoError(t, db.MPut(&Car{
			Id:        0,
			Name:      "test",
			CreatedAt: time.Now(),
		}))
		car := &Car{
			Id: 1,
		}
		require.NoError(t, db.MGet(car))
		require.Equal(t, uint32(1), car.Id)
		require.Equal(t, "test", car.Name)

	})
}

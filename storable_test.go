package boltutil

import (
	"encoding/binary"
	"fmt"
	"math/rand"
	"time"
)

type Person struct {
	Id   string
	Name string
	Age  int
}

func (p *Person) Bucket() []byte {
	return []byte("person")
}

func (p *Person) Key() []byte {
	return []byte(p.Id)
}

type Car struct {
	Id        uint32
	Name      string
	CreatedAt time.Time
}

func (c *Car) Bucket() []byte {
	return []byte("car")
}

func (c *Car) Key() []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, c.Id)
	return ret
}

type Wind struct {
}

func (c *Wind) Bucket() []byte {
	return []byte(fmt.Sprintf("%v %v", time.Now(), rand.Int()))
}

func (c *Wind) Key() []byte {
	return []byte(fmt.Sprintf("%v %v", time.Now(), rand.Int()))
}

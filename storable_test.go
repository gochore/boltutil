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

func (p *Person) BoltBucket() []byte {
	return []byte("person")
}

func (p *Person) BoltKey() []byte {
	return []byte(p.Id)
}

type Car struct {
	Id        uint32
	Name      string
	CreatedAt time.Time
}

func (c *Car) BoltBucket() []byte {
	return []byte("car")
}

func (c *Car) BoltKey() []byte {
	ret := make([]byte, 4)
	binary.BigEndian.PutUint32(ret, c.Id)
	return ret
}

func (c *Car) BoltCoder() Coder {
	return XmlCoder{}
}

type Wind struct {
}

func (c *Wind) BoltBucket() []byte {
	return []byte(fmt.Sprintf("%v %v", time.Now(), rand.Int()))
}

func (c *Wind) BoltKey() []byte {
	return []byte(fmt.Sprintf("%v %v", time.Now(), rand.Int()))
}

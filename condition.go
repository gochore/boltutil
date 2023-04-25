package boltutil

import (
	"bytes"
)

type Condition struct {
	min, max           []byte
	prefix             []byte
	conditions         []func(k, v []byte) (skip bool, stop bool)
	storableConditions []func(obj Storable) (skip bool, stop bool)
}

func NewCondition() *Condition {
	return &Condition{}
}

func (c *Condition) SetRange(min, max []byte) *Condition {
	c.min = min
	c.max = max
	return c
}

func (c *Condition) SetPrefix(prefix []byte) *Condition {
	c.min = prefix
	c.max = make([]byte, len(prefix))
	copy(c.max, c.max)
	for i := len(c.max) - 1; i >= 0; i-- {
		if c.max[i] < 0xff {
			c.max[i]++
			break
		}
	}
	return c
}

func (c *Condition) AddCondition(f func(k, v []byte) (skip bool, stop bool)) *Condition {
	c.conditions = append(c.conditions, f)
	return c
}

func (c *Condition) AddStorableCondition(f func(obj Storable) (skip bool, stop bool)) *Condition {
	c.storableConditions = append(c.storableConditions, f)
	return c
}

func (c *Condition) seek() []byte {
	if c == nil {
		return nil
	}
	if c.prefix != nil && bytes.Compare(c.prefix, c.min) > 0 {
		return c.prefix
	}
	return c.min
}

func (c *Condition) goon(k []byte) bool {
	if k == nil {
		return false
	}
	if c == nil {
		return true
	}
	if len(c.max) > 0 && bytes.Compare(k, c.max) > 0 {
		return false
	}
	if len(c.prefix) > 0 && !bytes.HasPrefix(k, c.prefix) {
		return false
	}
	return true
}

func (c *Condition) getConditions() []func(k, v []byte) (skip bool, stop bool) {
	if c == nil {
		return nil
	}
	return c.conditions
}

func (c *Condition) getStorableConditions() []func(obj Storable) (skip bool, stop bool) {
	if c == nil {
		return nil
	}
	return c.storableConditions
}

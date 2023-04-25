package boltutil

import (
	"bytes"
)

type Filter struct {
	min, max        []byte
	prefix          []byte
	filters         []func(k, v []byte) (skip bool, stop bool)
	storableFilters []func(obj Storable) (skip bool, stop bool)
}

func NewFilter() *Filter {
	return &Filter{}
}

func (c *Filter) SetRange(min, max []byte) *Filter {
	c.min = min
	c.max = max
	return c
}

func (c *Filter) SetPrefix(prefix []byte) *Filter {
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

func (c *Filter) AddCondition(f func(k, v []byte) (skip bool, stop bool)) *Filter {
	c.filters = append(c.filters, f)
	return c
}

func (c *Filter) AddStorableCondition(f func(obj Storable) (skip bool, stop bool)) *Filter {
	c.storableFilters = append(c.storableFilters, f)
	return c
}

func (c *Filter) seek() []byte {
	if c == nil {
		return nil
	}
	if c.prefix != nil && bytes.Compare(c.prefix, c.min) > 0 {
		return c.prefix
	}
	return c.min
}

func (c *Filter) goon(k []byte) bool {
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

func (c *Filter) getConditions() []func(k, v []byte) (skip bool, stop bool) {
	if c == nil {
		return nil
	}
	return c.filters
}

func (c *Filter) getStorableConditions() []func(obj Storable) (skip bool, stop bool) {
	if c == nil {
		return nil
	}
	return c.storableFilters
}

package boltutil

import (
	"bytes"
	"reflect"
	"testing"
)

func TestGobCoder(t *testing.T) {
	c := GobCoder{}

	want := &Person{
		Id:   "jason",
		Name: "Jason Song",
		Age:  25,
	}

	buffer := bytes.NewBuffer(nil)
	if err := c.Encode(buffer, want); err != nil {
		t.Fatal(err)
	}

	t.Logf("%q", buffer.String())

	got := &Person{}
	if err := c.Decode(buffer, got); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestJsonCoder(t *testing.T) {
	want := &Person{
		Id:   "jason",
		Name: "Jason Song",
		Age:  25,
	}

	{
		c := JsonCoder{}

		buffer := bytes.NewBuffer(nil)
		if err := c.Encode(buffer, want); err != nil {
			t.Fatal(err)
		}

		t.Logf("%q", buffer.String())

		got := &Person{}
		if err := c.Decode(buffer, got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
	{
		c := JsonCoder{
			Intent: true,
		}

		buffer := bytes.NewBuffer(nil)
		if err := c.Encode(buffer, want); err != nil {
			t.Fatal(err)
		}

		t.Logf("%q", buffer.String())

		got := &Person{}
		if err := c.Decode(buffer, got); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
}

func TestXmlCoder(t *testing.T) {
	c := XmlCoder{}

	want := &Person{
		Id:   "jason",
		Name: "Jason Song",
		Age:  25,
	}

	buffer := bytes.NewBuffer(nil)
	if err := c.Encode(buffer, want); err != nil {
		t.Fatal(err)
	}

	t.Logf("%q", buffer.String())

	got := &Person{}
	if err := c.Decode(buffer, got); err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

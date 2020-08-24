package boltutil

import (
	"encoding/gob"
	"encoding/json"
	"encoding/xml"
	"io"
)

// Coder is the interface that can encode and decode data.
type Coder interface {
	Encode(writer io.Writer, v interface{}) error
	Decode(reader io.Reader, v interface{}) error
}

// GobCoder implements Coder with gob
type GobCoder struct {
}

func (c GobCoder) Encode(writer io.Writer, v interface{}) error {
	return gob.NewEncoder(writer).Encode(v)
}

func (c GobCoder) Decode(reader io.Reader, v interface{}) error {
	return gob.NewDecoder(reader).Decode(v)
}

// JsonCoder implements Coder with json
type JsonCoder struct {
}

func (c JsonCoder) Encode(writer io.Writer, v interface{}) error {
	return json.NewEncoder(writer).Encode(v)
}

func (c JsonCoder) Decode(reader io.Reader, v interface{}) error {
	return json.NewDecoder(reader).Decode(v)
}

// XmlCoder implements Coder with xml
type XmlCoder struct {
}

func (c XmlCoder) Encode(writer io.Writer, v interface{}) error {
	return xml.NewEncoder(writer).Encode(v)
}

func (c XmlCoder) Decode(reader io.Reader, v interface{}) error {
	return xml.NewDecoder(reader).Decode(v)
}
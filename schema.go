package ghost

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
)

// Schema specifies how an object in a store is to be marshaled and unmarshaled.
type Schema interface {
	Marshal(o interface{}) ([]byte, error)
	Unmarshal(b []byte, o interface{}) error
}

// GobSchema is a generic schema for storing any kind of Go type in a store.
type GobSchema struct {
	object interface{}
}

type Float64Schema struct {
	value float64
}

func (GobSchema) Marshal(o interface{}) ([]byte, error) {
	buff := new(bytes.Buffer)
	err := gob.NewEncoder(buff).Encode(o)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (GobSchema) Unmarshal(b []byte, o interface{}) error {
	return gob.NewDecoder(bytes.NewReader(b)).Decode(o)
}

func NewGobSchema(o interface{}) *GobSchema {
	return &GobSchema{
		object: o,
	}
}

func (Float64Schema) Marshal(o interface{}) ([]byte, error) {
	var buf bytes.Buffer
	err := binary.Write(&buf, binary.BigEndian, o)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (Float64Schema) Unmarshal(b []byte, o interface{}) error {
	return binary.Read(bytes.NewBuffer(b), binary.BigEndian, &o)
}

func NewAtomicSchema(i interface{}) Schema {
	switch v := i.(type) {
	case float64:
		return &Float64Schema{
			value: v,
		}
	}
	return nil
}

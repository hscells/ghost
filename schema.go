package ghost

import (
	"bytes"
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

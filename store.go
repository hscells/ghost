package ghost

import (
	"encoding/gob"
	"fmt"
	"github.com/BurntSushi/toml"
	"github.com/google/uuid"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

type meta struct {
	Index  int   `toml:"index"`
	Len    int   `toml:"len"`
	Offset int64 `toml:"offset"`
}

type info struct {
	Inserts  int             `toml:"inserts"`
	Capacity int             `toml:"capacity"`
	Size     int64           `toml:"size"`
	Index    []string        `toml:"indexes"`
	Store    map[string]meta `toml:"store"`
}

type Store struct {
	inserts  int             // What is the size of the current Index?
	capacity int             // What is the maximum inserts of an Index?
	size     int64           // How many bytes have been written so far to the current Index?
	schema   Schema          // What is the type of data that is stored?
	index    []string        // What are the names of the indexes?
	store    map[string]meta // Which Index is the document contained in?

	dir string
	mu  sync.Mutex
}

func indexName() string {
	return uuid.New().String()
}

func NewStore(dir string, schema Schema) *Store {
	fmt.Println("new store")
	gob.Register(schema)
	gob.Register(Store{})
	rand.Seed(time.Now().UnixNano())
	return &Store{
		store:    make(map[string]meta),
		inserts:  0,
		capacity: 1e3,
		index:    []string{indexName()},
		schema:   schema,
		dir:      dir,
	}
}

func Open(dir string, schema Schema) (*Store, error) {
	confPath := path.Join(dir, "conf")
	if _, err := os.Stat(confPath); os.IsNotExist(err) {
		return NewStore(dir, schema), nil
	}

	gob.Register(schema)
	gob.Register(Store{})
	f, err := os.OpenFile(confPath, os.O_RDONLY, 0664)
	if err != nil {
		return nil, err
	}

	var info info

	_, err = toml.DecodeReader(f, &info)
	if err != nil {
		return nil, err
	}

	if info.Store == nil {
		info.Store = make(map[string]meta)
	}

	fmt.Println("open:", info)

	return &Store{
		store:    info.Store,
		inserts:  info.Inserts,
		capacity: info.Capacity,
		index:    info.Index,
		schema:   schema,
		dir:      dir,
	}, nil

}

func (s *Store) Commit() error {
	f, err := os.OpenFile(path.Join(s.dir, "conf"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}

	var info info

	s.mu.Lock()
	info.Store = s.store
	info.Index = s.index
	info.Capacity = s.capacity
	info.Inserts = s.inserts
	info.Size = s.size
	s.mu.Unlock()

	fmt.Println(info)

	return toml.NewEncoder(f).Encode(info)
}

func (s *Store) Insert(id string, o interface{}) error {
	b, err := s.schema.Marshal(o)
	if err != nil {
		return err
	}

	s.mu.Lock()

	// Reset the inserts if above the maximum.
	if s.inserts >= s.capacity {
		s.inserts = 0
		s.index = append(s.index, indexName())
	}

	// Get the current Index.
	idx := len(s.index) - 1

	// Open the Index for appending.
	f, err := os.OpenFile(path.Join(s.dir, s.index[idx]), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0664)
	if err != nil {
		return err
	}

	// Write the meta to the Index.
	n, err := f.Write(b)
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	// Insert the meta into the store.
	s.store[id] = meta{
		Index:  idx,
		Offset: s.size,
		Len:    len(b),
	}

	// Track how many bytes have been written so far.
	s.size += int64(n)
	s.inserts++

	s.mu.Unlock()

	return nil
}

func (s *Store) Get(id string, o interface{}) error {

	var (
		obj meta
		ok  bool
	)

	s.mu.Lock()

	// Get the meta-data about the object from the store.
	if obj, ok = s.store[id]; !ok {
		o = nil
		return nil
	}

	s.mu.Unlock()

	// Open the Index the object is stored in.
	f, err := os.OpenFile(path.Join(s.dir, s.index[obj.Index]), os.O_RDONLY, 0664)
	if err != nil {
		return err
	}

	// Read the object from disk.
	b := make([]byte, obj.Len)
	_, err = f.ReadAt(b, obj.Offset)
	if err != nil {
		return err
	}

	err = f.Close()
	if err != nil {
		return err
	}

	// Create a concrete go object.
	return s.schema.Unmarshal(b, o)
}

func (s *Store) Contains(id string) bool {
	_, ok := s.store[id]
	return ok
}

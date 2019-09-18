package ghost

import (
	"encoding/json"
	"fmt"
	"github.com/go-errors/errors"
	"io/ioutil"
	"math/rand"
	"os"
	"path"
	"sync"
	"time"
)

type info struct {
	Inserts  int      `json:"inserts"`
	Capacity int      `json:"capacity"`
	Size     int      `json:"size"`
	Index    []string `json:"indexes"`
	Stores   []string `json:"stores"`
}

type workingIndex struct {
	idx int
	set bool
	index
}

type workingStore struct {
	idx  int
	set  bool
	data []byte
}

// Store
type Store struct {
	inserts  int      // What is the size of the current Index?
	capacity int      // What is the maximum inserts of an Index?
	size     int      // How many bytes have been written so far to the current Index?
	schema   Schema   // What is the type of data that is stored?
	index    []string // What are the names of the indexes? (meta-data).
	store    []string // Which store is the document contained in? (object store).

	indexCache *IndexCache
	files      map[string]*os.File

	workingIndex workingIndex
	workingStore workingStore
	identifiers  map[identifier]bool

	dir string
	mu  sync.Mutex
}

func (s *Store) indexName() string {
	return fmt.Sprintf("g_%d%d%d%d%d%d", len(s.index), len(s.store), len(s.index)+len(s.store), s.capacity, rand.Int(), rand.Int())
}

type StoreOption func(s *Store)

// WithIndexCache will cache identifiers randomly with the specified capacity.
func WithIndexCache(capacity int) StoreOption {
	return func(s *Store) {
		s.indexCache = NewIndexCache(capacity)
	}
}

// NewStore creates a new object store.
func NewStore(dir string, schema Schema, options ...StoreOption) *Store {
	rand.Seed(time.Now().UnixNano())
	s := &Store{
		inserts:     0,
		capacity:    1e5,
		schema:      schema,
		dir:         dir,
		files:       make(map[string]*os.File),
		identifiers: make(map[identifier]bool),
	}

	s.index = []string{s.indexName()}
	s.store = []string{s.indexName()}

	for _, option := range options {
		option(s)
	}

	return s
}

// Close flushes the store and closes all open file pointers.
func (s *Store) Close() error {
	err := s.Flush()
	if err != nil {
		return err
	}
	for _, f := range s.files {
		err := f.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) loadIdentifiers() error {
	for _, i := range s.index {
		idx, err := readIndex(path.Join(s.dir, i))
		if err != nil {
			return err
		}
		for id := range idx {
			s.identifiers[id] = true
		}
	}

	return nil
}

// Open loads an object store if one exists, or creates one if one is not found.
func Open(dir string, schema Schema, options ...StoreOption) (*Store, error) {
	err := os.MkdirAll(dir, 0774)
	if err != nil {
		return nil, err
	}

	confPath := path.Join(dir, "conf")
	if _, err := os.Stat(confPath); os.IsNotExist(err) {
		return NewStore(dir, schema), nil
	}

	f, err := os.OpenFile(confPath, os.O_RDONLY, 0664)
	if err != nil {
		return nil, err
	}

	var info info

	err = json.NewDecoder(f).Decode(&info)
	if err != nil {
		return nil, err
	}

	s := &Store{
		store:       info.Stores,
		inserts:     info.Inserts,
		capacity:    info.Capacity,
		index:       info.Index,
		size:        info.Size,
		schema:      schema,
		dir:         dir,
		files:       make(map[string]*os.File),
		identifiers: make(map[identifier]bool),
	}

	for _, option := range options {
		option(s)
	}

	s.files[confPath] = f

	err = s.loadIdentifiers()
	if err != nil {
		return nil, err
	}

	return s, nil
}

// Flush commits changes to the store to disk (importantly the most recently updated size and inserts values).
// This is called automatically after every put.
func (s *Store) Flush() error {
	f, err := os.OpenFile(path.Join(s.dir, "conf"), os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer f.Close()

	var info info

	info.Stores = s.store
	info.Index = s.index
	info.Capacity = s.capacity
	info.Inserts = s.inserts
	info.Size = s.size

	return json.NewEncoder(f).Encode(info)
}

// Put writes an object with the specified id. A Put does not override existing objects with the same id, only
// an append operation is made. The previous location of the object, however, is lost.
func (s *Store) Put(id string, o interface{}) error {
	b, err := s.schema.Marshal(o)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Reset the inserts if above the maximum.
	if s.inserts >= s.capacity {
		s.inserts = 0
		s.size = 0
		s.index = append(s.index, s.indexName())
		s.store = append(s.store, s.indexName())
	}

	// Get the current index.
	idx := len(s.index) - 1
	idxPath := path.Join(s.dir, s.store[idx])

	var f *os.File
	if v, ok := s.files[idxPath]; ok {
		f = v
	} else {
		// Open the Store for appending.
		var err error
		f, err = os.OpenFile(idxPath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			return err
		}
		s.files[idxPath] = f
	}

	// Append the object to the store.
	n, err := f.WriteAt(b, int64(s.size))
	if err != nil {
		return err
	}

	// Put the meta into the index.
	m := meta{
		Index:  idx,
		Offset: s.size,
		Len:    n,
	}
	err = s.PutMeta(identifier(id), m)
	if err != nil {
		return err
	}

	s.identifiers[identifier(id)] = true

	if s.indexCache != nil {
		s.indexCache.Put(identifier(id), m)
	}

	// Track how many bytes have been written so far.
	s.size += n
	s.inserts++

	err = s.Flush()
	if err != nil {
		return err
	}

	return nil
}

func (s *Store) PutAll(ids []string, o []interface{}) error {
	if len(ids) != len(o) {
		return errors.Errorf("when putting many objects, slices must be of same length")
	}
	for i := 0; i < len(ids); i++ {
		err := s.Put(ids[i], o[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// Get retrieves an object with the specified id, if one exists.
func (s *Store) Get(id string, o interface{}) error {

	var (
		m *meta
	)

	if s.workingStore.idx == s.workingIndex.idx && s.workingStore.set && s.workingIndex.set {
		if m, ok := s.workingIndex.index[identifier(id)]; ok {
			if m.Offset+m.Len <= len(s.workingStore.data) {
				return s.schema.Unmarshal(s.workingStore.data[m.Offset:m.Offset+m.Len], o)
			}
		}
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.indexCache != nil {
		if v, ok := s.indexCache.lookup[identifier(id)]; ok {
			m = &s.indexCache.metadata[v]
		}
	}

	if m == nil {
		// Get the meta-data about the identifier from the index.
		var err error
		m, err = s.GetMeta(identifier(id))
		if err != nil {
			return err
		}
	}

	if m == nil {
		return nil
	}

	storePath := path.Join(s.dir, s.store[m.Index])
	var f *os.File
	if v, ok := s.files[storePath]; ok {
		f = v
	} else {
		// Open the Store for appending.
		var err error
		f, err = os.OpenFile(storePath, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0664)
		if err != nil {
			return err
		}
		s.files[storePath] = f
	}

	// Read the object from disk.
	b := make([]byte, m.Len)
	_, err := f.ReadAt(b, int64(m.Offset))
	if err != nil {
		return err
	}

	i, err := readIndex(path.Join(s.dir, s.index[m.Index]))
	if err != nil {
		panic(err)
	}
	data, err := ioutil.ReadAll(s.files[storePath])
	if err != nil {
		panic(err)
	}
	s.workingIndex = workingIndex{idx: m.Index, index: i, set: true}
	s.workingStore = workingStore{idx: m.Index, data: data, set: true}

	return s.schema.Unmarshal(b, o)
}

//func (s *Store) BulkGet(id []string, o []interface{}) error {
//	indexes := make([]index, len(s.index))
//
//	for i, index := range s.index {
//		b, err := ioutil.ReadFile(path.Join(s.dir, index))
//		if err != nil {
//			return err
//		}
//		err = json.Unmarshal(b, indexes[i])
//		if err != nil {
//			return err
//		}
//	}
//
//	for _, index := range indexes {
//
//	}
//}

// Contains checks to see if an object with the specified id is stored in the index.
func (s *Store) Contains(id string) bool {
	if _, ok := s.identifiers[identifier(id)]; ok {
		return true
	}

	if s.workingStore.idx == s.workingIndex.idx && s.workingStore.set && s.workingIndex.set {
		if _, ok := s.workingIndex.index[identifier(id)]; ok {
			return true
		}
	}

	return false
}

// Size retrieves the size in bytes of the current index.
func (s *Store) Size() int {
	return s.size
}

// Inserts retrieves the number of inserts made to the current index.
func (s *Store) Inserts(i int) int {
	return s.inserts
}

package ghost

import (
	"encoding/json"
	"io/ioutil"
	"os"
	"path"
	"sync"
)

type meta struct {
	Index  int `json:"index"`
	Len    int `json:"len"`
	Offset int `json:"offset"`
}

type identifier string

type index map[identifier]meta

func readIndex(name string) (index, error) {
	var i index
	if _, err := os.Stat(name); err == nil {
		b, err := ioutil.ReadFile(name)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(b, &i)
		if err != nil {
			panic(err)
		}
	} else {
		i = make(index)
	}
	return i, nil
}

func writeIndex(name string, id identifier, m meta) error {
	var (
		i index
		f *os.File
	)

	var err error
	f, err = os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0664)
	if err != nil {
		return err
	}

	if info, err := f.Stat(); err == nil && info.Size() == 0 {
		i = make(index)
	} else {
		b, err := ioutil.ReadAll(f)
		if err != nil {
			panic(err)
		}
		err = json.Unmarshal(b, &i)
		if err != nil {
			panic(err)
		}
	}

	i[id] = m
	b, err := json.Marshal(i)
	if err != nil {
		return err
	}

	err = f.Truncate(0)
	if err != nil {
		return err
	}
	_, err = f.Seek(0, 0)
	if err != nil {
		return err
	}

	_, err = f.Write(b)
	return err
}

// Put commits the metadata of an identifier to an index.
func (s *Store) PutMeta(id identifier, m meta) error {
	return writeIndex(path.Join(s.dir, s.index[len(s.index)-1]), id, m)
}

// Get retrieves the metadata of an identifier from the index it is stored in.
func (s *Store) GetMeta(id identifier) (*meta, error) {

	c := make(chan meta, 1)
	r := make(chan bool, len(s.index))

	var (
		once    sync.Once
		errOnce error
	)
	for _, indexName := range s.index {
		go func(idx string, m chan<- meta, results chan<- bool) {
			idxName := path.Join(s.dir, idx)
			var i index
			if f, ok := s.files[idxName]; ok {
				err := json.NewDecoder(f).Decode(&i)
				if err != nil {
					once.Do(func() {
						results <- false
						errOnce = err
					})
				}
			} else {
				var err error
				i, err = readIndex(idxName)
				if err != nil {
					once.Do(func() {
						results <- false
						errOnce = err
					})
				}
			}

			if v, ok := i[id]; ok {
				m <- v
				results <- true
				return
			}
			results <- false
			return
		}(indexName, c, r)
	}

	// Wait for all the goroutines to finish.
	for i := 0; i < len(s.index); i++ {
		found := <-r
		if found {
			m := <-c
			return &m, nil
		}
	}

	// If there was an error, quit.
	if errOnce != nil {
		return nil, errOnce
	}

	return nil, nil
}

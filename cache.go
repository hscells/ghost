package ghost

import (
	"math/rand"
	"sync"
)

type IndexCache struct {
	capacity int

	lookup   map[identifier]int
	metadata []meta

	mu sync.Mutex
}

func NewIndexCache(capacity int) *IndexCache {
	return &IndexCache{
		capacity: capacity,
		lookup:   make(map[identifier]int),
		metadata: make([]meta, capacity),
	}
}

func (i *IndexCache) Get(id identifier) *meta {
	i.mu.Lock()
	defer i.mu.Unlock()
	if v, ok := i.lookup[id]; ok {
		return &i.metadata[v]
	}
	return nil
}

func (i *IndexCache) Put(id identifier, m meta) {
	i.mu.Lock()
	defer i.mu.Unlock()
	idx := rand.Intn(i.capacity)
	i.metadata[idx] = m
	i.lookup[id] = idx
}

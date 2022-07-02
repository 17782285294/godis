package lock

import (
	"github.com/hdt3213/godis/datastruct/lock"
	"sort"
	"sync"
)

const prime32 = uint32(16777619)

// Locks provides rw locks for key
type Locks struct {
	table []*sync.RWMutex
}

func Make(tableSize int) *Locks {
	table := make([]*sync.RWMutex, tableSize)
	for i := 0; i < tableSize; i++ {
		table[i] = &sync.RWMutex{}
	}
	locker := &Locks{
		table: table,
	}
	return locker
}

func fnv32(key string) uint32 {
	hash := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		hash *= prime32
		hash ^= uint32(key[i])
	}
	return hash
}

func (locks *Locks) spread(hashCode uint32) uint32 {
	if locks == nil {
		panic("dict is nil")
	}
	tableSize := uint32(len(locks.table))
	return (tableSize - 1) & hashCode
}

// Lock obtains exclusive lock for writing
func (locks *Locks) Lock(key string) {
	hashCode := fnv32(key)
	index := locks.spread(hashCode)
	mutex := locks.table[index]
	mutex.Lock()
}

// RLock obtains shared lock for reading
func (locks *Locks) RLock(key string) {
	hashCode := fnv32(key)
	index := locks.spread(hashCode)
	mutex := locks.table[index]
	mutex.RLock()
}

// UnLock release exclusive lock
func (locks *Locks) UnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.Unlock()
}

// RUnLock release shared lock
func (locks *Locks) RUnLock(key string) {
	index := locks.spread(fnv32(key))
	mu := locks.table[index]
	mu.RUnlock()
}

func (locks *Locks) toLockIndices(keys []string, reverse bool) []uint32 {
	indexMap := make(map[uint32]bool)
	for _, key := range keys {
		hashCode := fnv32(key)
		index := locks.spread(hashCode)
		indexMap[index] = true
	}
	indices := make([]uint32, 0, len(indexMap))
	for index := range indexMap {
		indices = append(indices, index)
	}
	sort.Slice(indices, func(i, j int) bool {
		if reverse {
			return indices[i] < indices[j]
		}
		return indices[i] > indices[j]
	})
	return indices
}

// Locks obtains multiple exclusive locks for writing
// invoking Lock in loop may cause dead lock, please use Locks
func (locks *Locks) Locks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mutex := locks.table[index]
		mutex.Lock()
	}
}

// RLocks obtains multiple shared locks for reading
// invoking RLock in loop may cause dead lock, please use RLocks
func (locks *Locks) RLocks(keys ...string) {
	indices := locks.toLockIndices(keys, false)
	for _, index := range indices {
		mutex := locks.table[index]
		mutex.RLock()
	}
}

// UnLocks releases multiple exclusive locks
func (locks *Locks) UnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.Unlock()
	}
}

// RUnLocks releases multiple shared locks
func (locks *Locks) RUnLocks(keys ...string) {
	indices := locks.toLockIndices(keys, true)
	for _, index := range indices {
		mu := locks.table[index]
		mu.RUnlock()
	}
}

// RWLocks locks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWLocks(writeKeys, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys,false)
	writeIndexSet := make(map[uint32]struct{})
	for _, index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, ok := writeIndexSet[index]
		mutex := locks.table[index]
		if ok {
			mutex.Lock()
		} else {
			mutex.RLock()
		}
	}
}

// RWUnLocks unlocks write keys and read keys together. allow duplicate keys
func (locks *Locks) RWUnLocks(writeKeys, readKeys []string) {
	keys := append(writeKeys, readKeys...)
	indices := locks.toLockIndices(keys, false)
	writeIndices := locks.toLockIndices(writeKeys, false)
	writeIndexSet := make(map[uint32]struct{})
	for _, index := range writeIndices {
		writeIndexSet[index] = struct{}{}
	}
	for _, index := range indices {
		_, ok := writeIndexSet[index]
		mutex := locks.table[index]
		if ok {
			mutex.Unlock()
		} else {
			mutex.RUnlock()
		}
	}
}
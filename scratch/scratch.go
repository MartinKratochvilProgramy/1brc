package main

import (
	"fmt"
	"sync"
)

type SafeMap struct {
	data  map[string]int
	locks map[string]*sync.Mutex
	mutex sync.Mutex
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		data:  make(map[string]int),
		locks: make(map[string]*sync.Mutex),
	}
}

func (sm *SafeMap) Set(key string, value int) {
	sm.mutex.Lock()
	lock, ok := sm.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		sm.locks[key] = lock
	}
	sm.mutex.Unlock()

	lock.Lock()
	sm.data[key] = value
	lock.Unlock()
}

func (sm *SafeMap) Get(key string) int {
	sm.mutex.Lock()
	lock, ok := sm.locks[key]
	if !ok {
		lock = &sync.Mutex{}
		sm.locks[key] = lock
	}
	sm.mutex.Unlock()

	lock.Lock()
	defer lock.Unlock()
	return sm.data[key]
}

func main() {
	sm := NewSafeMap()

	// Launch multiple goroutines to update the map concurrently
	for i := 0; i < 10; i++ {
		go func(index int) {
			key := fmt.Sprintf("key%d", index)
			sm.Set(key, index) // Update the map with the value corresponding to the key
		}(i)
	}

	fmt.Println(sm)
}

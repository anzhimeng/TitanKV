package raftstore

import (
	"sort"
	"sync"
)

type Latches struct {
	mu    sync.Mutex
	locks map[string]chan struct{}
}

func NewLatches() *Latches {
	return &Latches{
		locks: make(map[string]chan struct{}),
	}
}

// Acquire acquires latches for the given keys.
// It returns a release function.
// This implementation uses key sorting to avoid deadlocks and per-key channels for efficient waiting.
func (l *Latches) Acquire(keys [][]byte) func() {
	if len(keys) == 0 {
		return func() {}
	}

	// 1. Deduplicate and Sort keys to prevent deadlock
	strKeys := make([]string, 0, len(keys))
	seen := make(map[string]struct{})
	for _, k := range keys {
		s := string(k)
		if _, ok := seen[s]; !ok {
			seen[s] = struct{}{}
			strKeys = append(strKeys, s)
		}
	}
	sort.Strings(strKeys)

	// 2. Acquire locks in order
	for _, k := range strKeys {
		for {
			l.mu.Lock()
			ch, locked := l.locks[k]
			if !locked {
				// Lock acquired
				l.locks[k] = make(chan struct{})
				l.mu.Unlock()
				break
			}
			// Wait
			l.mu.Unlock()
			<-ch
		}
	}

	return func() {
		l.mu.Lock()
		defer l.mu.Unlock()
		for _, k := range strKeys {
			if ch, ok := l.locks[k]; ok {
				close(ch) // Wake up waiters
				delete(l.locks, k)
			}
		}
	}
}

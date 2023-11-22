package zipserver

import (
	"sync"
	"time"
)

type LockTable struct {
	// maps aren't thread-safe in golang, this protects openKeys
	sync.Mutex
	// We're using the map to store the time at which a lock is obtained for a key
	openKeys map[string]time.Time
}

func NewLockTable() *LockTable {
	return &LockTable{
		openKeys: make(map[string]time.Time),
	}
}

// tryLockKey tries acquiring the lock for a given key
// it returns true if we successfully acquired the lock,
// false if the key is locked by someone else
func (lt *LockTable) tryLockKey(key string) bool {
	lt.Lock()
	defer lt.Unlock()

	// test for key existence
	if _, ok := lt.openKeys[key]; ok {
		// locked by someone else
		return false
	}
	lt.openKeys[key] = time.Now()
	return true
}

func (lt *LockTable) releaseKey(key string) {
	lt.Lock()
	defer lt.Unlock()

	// delete key from map so the map doesn't keep growing
	delete(lt.openKeys, key)
}

// GetLocks returns the keys currently held by the lock table
type KeyInfo struct {
	Key           string
	LockedAt      time.Time
	LockedSeconds float64
}

func (lt *LockTable) GetLocks() []KeyInfo {
	lt.Lock()
	defer lt.Unlock()

	keys := make([]KeyInfo, 0, len(lt.openKeys))
	for key, lockedAt := range lt.openKeys {
		keys = append(keys, KeyInfo{
			Key:           key,
			LockedAt:      lockedAt,
			LockedSeconds: time.Since(lockedAt).Seconds(),
		})
	}
	return keys
}

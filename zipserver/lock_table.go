package zipserver

import "sync"

type LockTable struct {
	// maps aren't thread-safe in golang, this protects openKeys
	sync.Mutex
	// empty struct is zero-width, we're using that map as a set (no values)
	openKeys map[string]struct{}
}

func NewLockTable() *LockTable {
	return &LockTable{
		openKeys: make(map[string]struct{}),
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
	lt.openKeys[key] = struct{}{}
	return true
}

func (lt *LockTable) releaseKey(key string) {
	lt.Lock()
	defer lt.Unlock()

	// delete key from map so the map doesn't keep growing
	delete(lt.openKeys, key)
}

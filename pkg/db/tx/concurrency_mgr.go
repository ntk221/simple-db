package tx

import "github.com/kawa1214/simple-db/pkg/db/file"

type ConcurrencyMgr struct {
	LockTable *LockTable
	Locks     map[file.BlockId]string
}

func NewConcurrencyMgr() *ConcurrencyMgr {
	lt := NewLockTable()
	return &ConcurrencyMgr{
		LockTable: lt,
		Locks:     make(map[file.BlockId]string),
	}
}

func (cm *ConcurrencyMgr) SLock(blk file.BlockId) {
	if _, exists := cm.Locks[blk]; !exists {
		cm.LockTable.SLock(blk)
		cm.Locks[blk] = "S"
	}
}

func (cm *ConcurrencyMgr) XLock(blk file.BlockId) {
	if !cm.HasXLock(blk) {
		cm.SLock(blk)
		cm.LockTable.XLock(blk)
		cm.Locks[blk] = "X"
	}
}

func (cm *ConcurrencyMgr) Release() {
	for blk := range cm.Locks {
		cm.LockTable.Unlock(blk)
		delete(cm.Locks, blk)
	}
}

func (cm *ConcurrencyMgr) HasXLock(blk file.BlockId) bool {
	lockType, exists := cm.Locks[blk]
	return exists && lockType == "X"
}

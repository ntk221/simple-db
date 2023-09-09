package file

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/util"
)

func TestNewFileMgr(t *testing.T) {
	mgr := newFileMgr(t)

	existMgr := NewManager(mgr.dbDirectory, 400)
	if existMgr.isNew != false {
		t.Errorf("mgr.isNew should be false")
	}
}

func newFileMgr(t *testing.T) *Manager {
	blockSize := 400
	name := util.RandomString(30)
	dir := fmt.Sprintf(".tmp/%s", name)
	mgr := NewManager(dir, blockSize)
	if mgr.dbDirectory != dir {
		t.Errorf("mgr.dbDirectory should be test")
	}
	if mgr.blockSize != blockSize {
		t.Errorf("mgr.blocksize should be 400")
	}
	if mgr.isNew != true {
		t.Errorf("mgr.isNew should be true")
	}
	if mgr.openFiles == nil {
		t.Errorf("mgr.openFiles should not be nil")
	}

	return mgr
}

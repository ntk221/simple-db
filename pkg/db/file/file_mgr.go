package file

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Manager はOSと連携してディスクブロックに対するページの読み書きを行う
// ファイルを用いてディスクに対する読み書きを実装している
//
// openFiles 内の各 *os.File オブジェクトは、それぞれオープンされたファイルに対応します。
type Manager struct {
	dbDirectory string
	blockSize   int
	isNew       bool
	openFiles   map[string]*os.File
	mu          sync.Mutex
}

func NewManager(dbDirectory string, blockSize int) *Manager {
	mgr := &Manager{
		dbDirectory: dbDirectory,
		blockSize:   blockSize,
		isNew:       !fileExists(dbDirectory),
		openFiles:   make(map[string]*os.File),
	}

	if mgr.isNew {
		os.MkdirAll(dbDirectory, 0755)
	}

	return mgr
}

// Read は指定されたファイル内の適切な位置に移動し、そのブロックの内容を指定されたページのバイトバッファに読み込む
func (mgr *Manager) Read(blk *BlockID, p *Page) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot read block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blockSize), 0)
	if err != nil {
		return err
	}

	_, err = f.Read(p.Contents().Bytes())
	return err
}

// Write は指定されたファイル内の適切な位置に移動し、そのブロックの場所にページの内容を書き込む
func (mgr *Manager) Write(blk *BlockID, p *Page) error {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return fmt.Errorf("cannot write block %v: %w", blk, err)
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blockSize), 0)
	if err != nil {
		return err
	}

	_, err = f.Write(p.Contents().Bytes())
	return err
}

// Append はファイルの末尾に移動し、空のバイト配列を書き込むことで、OSに自動的にファイルを拡張させる
// これによって、ファイルマネージャは常にファイルからブロックサイズのバイト数を読み取り、常にブロック境界で読み書きを行う
func (mgr *Manager) Append(filename string) (*BlockID, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	newBlkSize := mgr.size(filename)
	blk := NewBlockID(filename, newBlkSize)
	b := make([]byte, mgr.blockSize)

	f, err := mgr.getFile(blk.FileName())
	if err != nil {
		return nil, err
	}

	_, err = f.Seek(int64(blk.Number()*mgr.blockSize), 0)
	if err != nil {
		return nil, err
	}

	_, err = f.Write(b)
	if err != nil {
		return nil, err
	}

	return blk, nil
}

func (mgr *Manager) Length(filename string) (int, error) {
	mgr.mu.Lock()
	defer mgr.mu.Unlock()

	f, err := mgr.getFile(filename)
	if err != nil {
		return 0, err
	}

	length, err := f.Seek(0, 2) // Seek to the end of the file
	return int(length) / mgr.blockSize, err
}

func (mgr *Manager) IsNew() bool {
	return mgr.isNew
}

func (mgr *Manager) BlockSize() int {
	return mgr.blockSize
}

// fileは rws モードで開く
// sは、OSの最適化によるディスクアクセスの遅延を無効にして、ディスクアクセスが即座に行われるようにする指定
func (mgr *Manager) getFile(filename string) (*os.File, error) {
	if f, exists := mgr.openFiles[filename]; exists {
		return f, nil
	}

	filePath := filepath.Join(mgr.dbDirectory, filename)
	f, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0666) // rwsモードで開く
	if err != nil {
		return nil, err
	}

	mgr.openFiles[filename] = f
	return f, nil
}

func (mgr *Manager) size(filename string) int {
	f, err := mgr.getFile(filename)
	if err != nil {
		return -1
	}

	info, err := f.Stat()
	if err != nil {
		return -1
	}

	return int(info.Size()) / mgr.blockSize
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return !os.IsNotExist(err)
}

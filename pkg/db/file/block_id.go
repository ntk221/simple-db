package file

import (
	"fmt"
)

// BlockID represents a block identifier
type BlockID struct {
	filename string
	blkNum   int
}

func NewBlockID(filename string, blkNum int) *BlockID {
	return &BlockID{
		filename: filename,
		blkNum:   blkNum,
	}
}

func (b *BlockID) FileName() string {
	return b.filename
}

// Returns the block number, which is the identifier of the block.
func (b *BlockID) Number() int {
	return b.blkNum
}

func (b *BlockID) Equal(other *BlockID) bool {
	return b.filename == other.filename && b.blkNum == other.blkNum
}

func (b *BlockID) String() string {
	return fmt.Sprintf("[file %s, block %d]", b.filename, b.blkNum)
}

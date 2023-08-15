package index

import (
	"fmt"
	"testing"

	"github.com/kawa1214/simple-db/pkg/db/buffer"
	"github.com/kawa1214/simple-db/pkg/db/file"
	"github.com/kawa1214/simple-db/pkg/db/log"
	"github.com/kawa1214/simple-db/pkg/db/metadata"
	"github.com/kawa1214/simple-db/pkg/db/plan"
	"github.com/kawa1214/simple-db/pkg/db/record"
	"github.com/kawa1214/simple-db/pkg/db/tx"
	"github.com/kawa1214/simple-db/pkg/util"
)

func TestIndexJoin(t *testing.T) {
	rootDir := util.ProjectRootDir()
	dir := rootDir + "/.tmp/studentdb"
	fm := file.NewFileMgr(dir, 400)
	lm := log.NewLogMgr(fm, "testlogfile")
	bm := buffer.NewBufferMgr(fm, lm, 8)

	tx := tx.NewTransaction(fm, lm, bm)
	mdm := metadata.NewMetadataMgr(true, tx)

	// Find the index on StudentId.
	indexes := mdm.GetIndexInfo("enroll", tx)
	sidIdx := indexes["studentid"]

	// Get plans for the Student and Enroll tables
	studentplan := plan.NewTablePlan(tx, "student", mdm)
	enrollplan := plan.NewTablePlan(tx, "enroll", mdm)

	// Two different ways to use the index in simpledb:
	useIndexJoinManually(studentplan, enrollplan, sidIdx, "sid")
	useIndexJoinScan(studentplan, enrollplan, sidIdx, "sid")

	tx.Commit()
}

func useIndexJoinManually(p1, p2 plan.Plan, ii *metadata.IndexInfo, joinfield string) {
	// Open scans on the tables.
	s1 := p1.Open()
	s2, ok := p2.Open().(*record.TableScan) // must be a table scan
	if !ok {
		panic("Expected a TableScan")
	}
	idx := Open(ii)

	// Loop through s1 records. For each value of the join field,
	// use the index to find the matching s2 records.
	for s1.Next() {
		c := s1.GetVal(joinfield)
		idx.BeforeFirst(c)
		for idx.Next() {
			// Use each datarid to go to the corresponding Enroll record.
			datarid := idx.GetDataRid()
			s2.MoveToRid(datarid) // table scans can move to a specified RID.
			fmt.Println(s2.GetString("grade"))
		}
	}
	idx.Close()
	s1.Close()
	s2.Close()
}

func useIndexJoinScan(p1, p2 plan.Plan, ii *metadata.IndexInfo, joinfield string) {
	// Open an index join scan on the table.
	idxplan := NewIndexJoinPlan(p1, p2, ii, joinfield)
	s := idxplan.Open()

	for s.Next() {
		fmt.Println(s.GetString("grade"))
	}
	s.Close()
}

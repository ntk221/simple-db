package query

import (
	"fmt"

	"github.com/kawa1214/simple-db/pkg/db/record"
)

type ProjectScan struct {
	s         Scan
	fieldlist []string
}

func NewProjectScan(s Scan, fieldlist []string) *ProjectScan {
	return &ProjectScan{
		s:         s,
		fieldlist: fieldlist,
	}
}

func (ps *ProjectScan) BeforeFirst() {
	ps.s.BeforeFirst()
}

func (ps *ProjectScan) Next() bool {
	return ps.s.Next()
}

func (ps *ProjectScan) GetInt(fldname string) (int, error) {
	if ps.HasField(fldname) {
		return ps.s.GetInt(fldname), nil
	}
	return 0, fmt.Errorf(fmt.Sprintf("field %s not found", fldname))
}

func (ps *ProjectScan) GetString(fldname string) (string, error) {
	if ps.HasField(fldname) {
		return ps.s.GetString(fldname), nil
	}
	return "", fmt.Errorf(fmt.Sprintf("field %s not found", fldname))
}

func (ps *ProjectScan) GetVal(fldname string) (*record.Constant, error) {
	if ps.HasField(fldname) {
		return ps.s.GetVal(fldname), nil
	}
	return nil, fmt.Errorf(fmt.Sprintf("field %s not found", fldname))
}

func (ps *ProjectScan) HasField(fldname string) bool {
	for _, field := range ps.fieldlist {
		if field == fldname {
			return true
		}
	}
	return false
}

func (ps *ProjectScan) Close() {
	ps.s.Close()
}

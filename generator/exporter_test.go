package generator

import "testing"

func TestRecord(t *testing.T) {
	Record(Table{})
	Record(Job{})
	Record(StartEdge{})
	Record(EndEdge{})
	Record(InheritEdge{})
}

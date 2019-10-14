package generator

import (
	"fmt"
)

// What's relationship about cluster/database/dataset/table

type User struct {
	Source   string
	Username string
}

type Cluster struct {
	Name string
}

type Database struct {
	VID  int64
	DbId string
}

type Table struct {
	VID       int64
	DatasetId string
	Cluster   string
	TableName string
	Source    string
}

type Job struct {
	VID           int64
	JobId         string
	JobServerIp   string
	HiveUser      string
	OperationName string
	JobType       string
	StartTime     int64
	EndTime       int64
}

type StartEdge struct {
	SrcTableVID int64
	JobVID      int64
	StartTime   int64
	EndTime     int64
}

type EndEdge struct {
	JobVID      int64
	DstTableVID int64
	StartTime   int64
	EndTime     int64
}

type InheritEdge struct {
	SrcTableVID int64
	DstTableVID int64
	JobVID      int64
	StartTime   int64
	EndTime     int64
}

type ContainEdge struct {
	DbVID    int64
	TableVID int64
}

type ReverseContainEdge struct {
	TableVID int64
	DbVID    int64
}

func (db *Database) String() string {
	return fmt.Sprintf("db(vid: %d, db_id:%s)", db.VID, db.DbId)
}

func (t *Table) String() string {
	return fmt.Sprintf("table(vid:%d, dataset:%s, name:%s, cluster:%s, source:%s)",
		t.VID, t.DatasetId, t.TableName, t.Cluster, t.Source)
}

func (j *Job) String() string {
	return fmt.Sprintf("job(vid:%d, id:%s, serverip:%s, hiveuser:%s, op:%s, type:%s, start:%d, end:%d)",
		j.VID, j.JobId, j.JobServerIp, j.HiveUser, j.OperationName, j.JobType, j.StartTime, j.EndTime)
}

func (e *StartEdge) String() string {
	return fmt.Sprintf("start: %d -> %d, start:%d, end:%d", e.SrcTableVID, e.JobVID, e.StartTime, e.EndTime)
}

func (e *EndEdge) String() string {
	return fmt.Sprintf("end: %d -> %d, start: %d, end: %d", e.JobVID, e.DstTableVID, e.StartTime, e.EndTime)
}

func (e *InheritEdge) String() string {
	return fmt.Sprintf("inherit: %d -> %d, job: %d, start: %d, end: %d",
		e.SrcTableVID, e.DstTableVID, e.JobVID, e.StartTime, e.EndTime)
}

func (e *ContainEdge) String() string {
	return fmt.Sprintf("contain: %d -> %d", e.DbVID, e.TableVID)
}

func (e *ReverseContainEdge) String() string {
	return fmt.Sprintf("contain: %d -> %d", e.TableVID, e.DbVID)
}

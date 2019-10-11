package generator

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"github.com/google/uuid"
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
	DbId int64
}

type Table struct {
	VID       int64
	DatasetId int64
	DbId      int64
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

const (
	UserCount      = 20
	ClusterCount   = 100
	DbCount        = 1000            // 1k
	DatasetCount   = 50 * 10 * 1000  // 500k
	JobCount       = 200 * 10 * 1000 // 2M
	StartEdgeCount = 500 * 10 * 1000 // 5M
	EndEdgeCount   = 300 * 10 * 1000 // 3M
)

func (t *Table) String() string {
	return fmt.Sprintf("table(vid:%d, dataset:%d, db:%d, name:%s, cluster:%s, source:%s)",
		t.VID, t.DatasetId, t.DbId, t.TableName, t.Cluster, t.Source)
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

func GenerateUsers(size int) []User {
	users := make([]User, size)
	for idx := range users {
		users[idx] = User{
			Source:   "Hive",
			Username: fmt.Sprintf("u%d", idx),
		}
	}
	return users
}

func GenerateCluster(size int64) []Cluster {
	clusters := make([]Cluster, size)
	for idx := range clusters {
		clusters[idx] = Cluster{
			Name: fmt.Sprintf("j%d", idx),
		}
	}
	return clusters
}

func GenerateDatabases(size int64) []Database {
	databases := make([]Database, size)
	for idx := range databases {
		databases[idx] = Database{
			DbId: rand.Int63n(size),
		}
	}
	return databases
}

func GenerateTables(size int64, databases []Database, clusters []Cluster, users []User) []Table {
	tables := make([]Table, size)
	for idx := range tables {
		dbId := rand.Intn(len(databases))
		clusterId := rand.Intn(len(clusters))
		userId := rand.Intn(len(users))
		vid := int64(idx)
		tables[idx] = Table{
			VID:       vid,
			DatasetId: vid,
			DbId:      databases[dbId].DbId,
			Cluster:   clusters[clusterId].Name,
			TableName: fmt.Sprintf("table%d", idx),
			Source:    users[userId].Source,
		}

		log.Println(tables[idx].String())
	}
	return tables
}

var JobServerIps = [...]string{"11.36.96.2", "11.36.96.3", "11.36.96.4", "11.36.96.5"}
var OperationNames = [...]string{"QUERY", "DDL", "DML"}
var JobTypes = [...]string{"hive", "mysql"}

func GenerateJobs(size int64, users []User) []Job {
	jobs := make([]Job, size)
	startTime := time.Now().Unix()
	var endTime int64
	for idx := range jobs {
		vid := int64(idx)
		uuid := uuid.New()
		userId := rand.Intn(len(users))
		jobServerIpIdx := rand.Intn(len(JobServerIps))
		opNameIdx := rand.Intn(len(OperationNames))
		jobTypeIdx := rand.Intn(len(JobTypes))
		endTime = startTime + rand.Int63n(1024)
		jobs[idx] = Job{
			VID:           vid,
			JobId:         strings.Replace(uuid.String(), "-", "", -1),
			JobServerIp:   JobServerIps[jobServerIpIdx],
			HiveUser:      users[userId].Username,
			OperationName: OperationNames[opNameIdx],
			JobType:       JobTypes[jobTypeIdx],
			StartTime:     startTime,
			EndTime:       endTime,
		}

		startTime = endTime + rand.Int63n(2048)
		log.Println(jobs[idx].String())
	}
	return jobs
}

func GenerateStartEndEdges(tables []Table, jobs []Job) (startEdges []StartEdge, endEdges []EndEdge) {
	for _, job := range jobs {
		numInEdges := rand.Intn(10)
		for i := 0; i < numInEdges; i++ {
			tblIdx := rand.Intn(len(tables))
			startEdge := StartEdge{
				SrcTableVID: tables[tblIdx].VID,
				JobVID:      job.VID,
				StartTime:   job.StartTime,
				EndTime:     job.EndTime,
			}
			startEdges = append(startEdges, startEdge)
			log.Println(startEdge.String())
		}

		numOutEdges := rand.Intn(10)
		for i := 0; i < numOutEdges; i++ {
			tblIdx := rand.Intn(len(tables))
			endEdge := EndEdge{
				JobVID:      job.VID,
				DstTableVID: tables[tblIdx].VID,
				StartTime:   job.StartTime,
				EndTime:     job.EndTime,
			}
			endEdges = append(endEdges, endEdge)
			log.Println(endEdge.String())
		}
	}

	return startEdges, endEdges
}

func GenerateInhritEdges(tables []Table, jobs []Job, startEdges []StartEdge, endEdges []EndEdge) (inheritEdges []InheritEdge) {
	for _, startEdge := range startEdges {
		srcTableVID := startEdge.SrcTableVID
		jobVID := startEdge.JobVID

		job, _ := GetJobByVID(jobs, jobVID)
		for _, dstTable := range GetDstTableByJobVID(endEdges, tables, jobVID) {
			inheritEdge := InheritEdge{
				SrcTableVID: srcTableVID,
				DstTableVID: dstTable.VID,
				JobVID:      jobVID,
				StartTime:   job.StartTime,
				EndTime:     job.EndTime,
			}
			inheritEdges = append(inheritEdges, inheritEdge)
			log.Println(inheritEdge.String())
		}
	}
	return inheritEdges
}

func GetTableByVID(tables []Table, vid int64) (Table, error) {
	for _, table := range tables {
		if table.VID == vid {
			return table, nil
		}
	}
	return Table{}, errors.New(fmt.Sprintf("Invalid table vid: %d", vid))
}

func GetJobByVID(jobs []Job, vid int64) (Job, error) {
	for _, job := range jobs {
		if job.VID == vid {
			return job, nil
		}
	}
	return Job{}, errors.New(fmt.Sprintf("Invalid job vid: %d", vid))
}

func GetDstTableByJobVID(endEdges []EndEdge, tables []Table, jobVID int64) []Table {
	var results []Table
	for _, endEdge := range endEdges {
		if endEdge.JobVID == jobVID {
			dstTable, _ := GetTableByVID(tables, endEdge.DstTableVID)
			results = append(results, dstTable)
		}
	}
	return results
}

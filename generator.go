package nebula_poc

import (
	"fmt"
	"math/rand"
	"time"

	"github.com/google/uuid"
)

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
	UserCount    = 20
	ClusterCount = 100
	DbCount      = 1000            // 1k
	DatasetCount = 50 * 10 * 1000  // 500k
	JobCount     = 200 * 10 * 1000 // 2M
)

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
			JobId:         uuid.String(),
			JobServerIp:   JobServerIps[jobServerIpIdx],
			HiveUser:      users[userId].Username,
			OperationName: OperationNames[opNameIdx],
			JobType:       JobTypes[jobTypeIdx],
			StartTime:     startTime,
			EndTime:       endTime,
		}

		startTime = endTime + rand.Int63n(2048)
	}
	return jobs
}

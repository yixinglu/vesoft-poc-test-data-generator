package generator

import (
	"errors"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
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
		vid := rand.Int63n(size)
		databases[idx] = Database{
			VID:  vid,
			DbId: fmt.Sprintf("%d", vid),
		}
	}
	log.Printf("Finish generate databases: %d", len(databases))
	return databases
}

func GenerateTables(size int64, clusters []Cluster, users []User) []Table {
	tables := make([]Table, size)
	for idx := range tables {
		clusterId := rand.Intn(len(clusters))
		userId := rand.Intn(len(users))
		vid := int64(idx)
		tables[idx] = Table{
			VID:       vid,
			DatasetId: fmt.Sprintf("%d", vid),
			Cluster:   clusters[clusterId].Name,
			TableName: fmt.Sprintf("table%d", idx),
			Source:    users[userId].Source,
		}

		// log.Println(tables[idx].String())
	}

	log.Printf("Finish generate tables: %d", len(tables))
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
		// log.Println(jobs[idx].String())
	}

	log.Printf("Finish generate jobs: %d", len(jobs))
	return jobs
}

func GenerateContainEdge(tables []Table, databases []Database) (containEdges []ContainEdge, reverseContainEdges []ReverseContainEdge) {
	for _, table := range tables {
		dbVID := rand.Intn(len(databases))
		containEdge := ContainEdge{
			DbVID:    databases[dbVID].VID,
			TableVID: table.VID,
		}
		containEdges = append(containEdges, containEdge)
		// log.Println(containEdge.String())
		reverseContainEdge := ReverseContainEdge{
			TableVID: table.VID,
			DbVID:    databases[dbVID].VID,
		}
		reverseContainEdges = append(reverseContainEdges, reverseContainEdge)
		// log.Println(reverseContainEdge.String())
	}
	log.Printf("Finish generate contain edges: %d, reverse contain edges: %d", len(containEdges), len(reverseContainEdges))
	return containEdges, reverseContainEdges
}

func GenerateEdges(tables []Table, jobs []Job) (startEdges []StartEdge, endEdges []EndEdge, inheritEdges []InheritEdge) {
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
			// log.Println(startEdge.String())
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
			// log.Println(endEdge.String())
		}

		for _, startEdge := range startEdges[len(startEdges)-numInEdges:] {
			for _, endEdge := range endEdges[len(endEdges)-numOutEdges:] {
				inheritEdge := InheritEdge{
					SrcTableVID: startEdge.SrcTableVID,
					DstTableVID: endEdge.DstTableVID,
					JobVID:      job.VID,
					StartTime:   job.StartTime,
					EndTime:     job.EndTime,
				}
				inheritEdges = append(inheritEdges, inheritEdge)
				// log.Println(inheritEdge.String())
			}
		}
	}

	log.Printf("Finish generate start edges(%d), end edges(%d) and inherit edges(%d)",
		len(startEdges), len(endEdges), len(inheritEdges))
	return startEdges, endEdges, inheritEdges
}

func GenerateInhritEdges(tables []Table, jobs []Job, startEdges []StartEdge, endEdges []EndEdge) (inheritEdges []InheritEdge) {
	for _, startEdge := range startEdges {
		srcTableVID := startEdge.SrcTableVID
		jobVID := startEdge.JobVID

		var wg sync.WaitGroup

		var job Job
		GetJobByVIDAsync(jobs, jobVID, &job, &wg)

		var dstTables []Table
		GetDstTablesByJobVIDAsync(endEdges, tables, jobVID, &dstTables, &wg)

		wg.Wait()

		for _, dstTable := range dstTables {
			inheritEdge := InheritEdge{
				SrcTableVID: srcTableVID,
				DstTableVID: dstTable.VID,
				JobVID:      jobVID,
				StartTime:   job.StartTime,
				EndTime:     job.EndTime,
			}
			inheritEdges = append(inheritEdges, inheritEdge)
			// log.Println(inheritEdge.String())
		}
	}
	log.Printf("Finish generate inherit edges: %d", len(inheritEdges))
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

func GetJobByVIDAsync(jobs []Job, vid int64, job *Job, wg *sync.WaitGroup) {
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		j, _ := GetJobByVID(jobs, vid)
		job = &j
		wg.Done()
	}(wg)
}

func GetJobByVID(jobs []Job, vid int64) (Job, error) {
	for _, job := range jobs {
		if job.VID == vid {
			return job, nil
		}
	}
	return Job{}, errors.New(fmt.Sprintf("Invalid job vid: %d", vid))
}

func GetDstTablesByJobVIDAsync(endEdges []EndEdge, tables []Table, jobVID int64, dstTables *[]Table, wg *sync.WaitGroup) {
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		dstTbls := GetDstTablesByJobVID(endEdges, tables, jobVID)
		dstTables = &dstTbls
		wg.Done()
	}(wg)
}

func GetDstTablesByJobVID(endEdges []EndEdge, tables []Table, jobVID int64) []Table {
	var results []Table
	for _, endEdge := range endEdges {
		if endEdge.JobVID == jobVID {
			dstTable, _ := GetTableByVID(tables, endEdge.DstTableVID)
			results = append(results, dstTable)
		}
	}
	return results
}

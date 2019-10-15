package main

import (
	"flag"
	"fmt"
	"sync"

	gen "github.com/yixinglu/vesoft-poc-test-data-generator/generator"
)

const (
	userCount    = 20
	clusterCount = 100
	dbCount      = 1000            // 1k
	datasetCount = 50 * 10 * 1000  // 500k
	jobCount     = 200 * 10 * 1000 // 2M
)

var dir = flag.String("d", "./", "specify the directory of csv data files")

func main() {
	flag.Parse()

	var prepareWG sync.WaitGroup

	// Cluster
	var clusters []gen.Cluster
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		clusters = gen.GenerateCluster(clusterCount)
		wg.Done()
	}(&prepareWG)

	// User
	var users []gen.User
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		users = gen.GenerateUsers(userCount)
		wg.Done()
	}(&prepareWG)

	prepareWG.Wait()

	var vertexWG, exportWG sync.WaitGroup

	// Database
	var databases []gen.Database
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		databases = gen.GenerateDatabases(dbCount)
		wg.Done()
	}(&vertexWG)

	// Table
	var tables []gen.Table
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		tables = gen.GenerateTables(datasetCount, clusters, users)
		wg.Done()
	}(&vertexWG)

	// Job
	var jobs []gen.Job
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		jobs = gen.GenerateJobs(jobCount, users)
		wg.Done()
	}(&vertexWG)

	vertexWG.Wait()

	gen.ExportDatabaseToCSVFile(fmt.Sprintf("%s/%s", *dir, "db.csv"), databases, &exportWG)
	gen.ExportTablesToCSVFile(fmt.Sprintf("%s/%s", *dir, "tbl.csv"), tables, &exportWG)
	gen.ExportJobsToCSVFile(fmt.Sprintf("%s/%s", *dir, "job.csv"), jobs, &exportWG)

	exportWG.Add(1)
	go generateAndExportDbRelatedEdges(tables, databases, &exportWG)

	exportWG.Add(1)
	go generateAndExportJobRelatedEdges(tables, jobs, &exportWG)

	exportWG.Wait()
}

func generateAndExportDbRelatedEdges(tables []gen.Table, databases []gen.Database, wg *sync.WaitGroup) {
	containEdges, reverseContainEdges := gen.GenerateContainEdge(tables, databases)
	var expWG sync.WaitGroup
	gen.ExportContainEdgesToCSVFile(fmt.Sprintf("%s/%s", *dir, "contain.csv"), containEdges, &expWG)
	gen.ExportReverseContainEdgesToCSVFile(fmt.Sprintf("%s/%s", *dir, "reverse-contain.csv"), reverseContainEdges, &expWG)

	expWG.Wait()

	wg.Done()
}

func generateAndExportJobRelatedEdges(tables []gen.Table, jobs []gen.Job, wg *sync.WaitGroup) {
	startEdges, endEdges, inheritEdges := gen.GenerateEdges(tables, jobs)

	var expWG sync.WaitGroup

	gen.ExportStartEdgesToCSVFile(fmt.Sprintf("%s/%s", *dir, "start.csv"), startEdges, &expWG)
	gen.ExportEndEdgesToCSVFile(fmt.Sprintf("%s/%s", *dir, "end.csv"), endEdges, &expWG)
	gen.ExportInheritEdgesToCSVFile(fmt.Sprintf("%s/%s", *dir, "inherit.csv"), inheritEdges, &expWG)

	expWG.Wait()

	wg.Done()
}

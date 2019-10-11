package main

import (
	"sync"

	gen "github.com/yixinglu/vesoft-poc-test-data-generator/generator"
)

const (
	userCount      = 20
	clusterCount   = 100
	DbCount        = 1000            // 1k
	datasetCount   = 50 * 10 * 1000  // 500k
	jobCount       = 200 * 10 * 1000 // 2M
	StartEdgeCount = 500 * 10 * 1000 // 5M
	EndEdgeCount   = 300 * 10 * 1000 // 3M
)

func main() {

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

	// Database
	var databases []gen.Database
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		databases = gen.GenerateDatabases(datasetCount)
		wg.Done()
	}(&prepareWG)

	prepareWG.Wait()

	var vertexWG, exportWG sync.WaitGroup

	// Table
	var tables []gen.Table
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		tables = gen.GenerateTables(datasetCount, databases, clusters, users)
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

	gen.ExportTablesToCSVFile("tables.csv", tables, &exportWG)
	gen.ExportJobsToCSVFile("jobs.csv", jobs, &exportWG)

	startEdges, endEdges, inheritEdges := gen.GenerateEdges(tables, jobs)

	gen.ExportStartEdgesToCSVFile("start.csv", startEdges, &exportWG)
	gen.ExportEndEdgesToCSVFile("end.csv", endEdges, &exportWG)
	gen.ExportInheritEdgesToCSVFile("inherit.csv", inheritEdges, &exportWG)

	exportWG.Wait()
}

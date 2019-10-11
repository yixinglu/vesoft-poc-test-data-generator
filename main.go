package main

import (
	"sync"

	gen "github.com/yixinglu/vesoft-poc-test-data-generator/generator"
)

func main() {

	var prepareWG sync.WaitGroup

	// Cluster
	var clusters []gen.Cluster
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		clusters = gen.GenerateCluster(gen.ClusterCount)
		wg.Done()
	}(&prepareWG)

	// User
	var users []gen.User
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		users = gen.GenerateUsers(gen.UserCount)
		wg.Done()
	}(&prepareWG)

	// Database
	var databases []gen.Database
	prepareWG.Add(1)
	go func(wg *sync.WaitGroup) {
		databases = gen.GenerateDatabases(gen.DatasetCount)
		wg.Done()
	}(&prepareWG)

	prepareWG.Wait()

	var vertexWG, exportWG sync.WaitGroup

	// Table
	var tables []gen.Table
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		tables = gen.GenerateTables(gen.DatasetCount, databases, clusters, users)
		wg.Done()
	}(&vertexWG)

	// Job
	var jobs []gen.Job
	vertexWG.Add(1)
	go func(wg *sync.WaitGroup) {
		jobs = gen.GenerateJobs(gen.JobCount, users)
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

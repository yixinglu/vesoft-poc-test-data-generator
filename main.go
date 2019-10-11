package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"

	gen "github.com/yixinglu/webank-poc-test-data-generator/generator"
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
	exportWG.Add(1)
	go func(wg *sync.WaitGroup, expWG *sync.WaitGroup) {
		tables = gen.GenerateTables(gen.DatasetCount, databases, clusters, users)
		wg.Done()
		exportTablesToCSVFile("tables.csv", tables)
		expWG.Done()
	}(&vertexWG, &exportWG)

	// Job
	var jobs []gen.Job
	vertexWG.Add(1)
	exportWG.Add(1)
	go func(wg *sync.WaitGroup, expWG *sync.WaitGroup) {
		jobs = gen.GenerateJobs(gen.JobCount, users)
		wg.Done()
		exportJobsToCSVFile("jobs.csv", jobs)
		expWG.Done()
	}(&vertexWG, &exportWG)

	vertexWG.Wait()

	startEdges, endEdges := gen.GenerateStartEndEdges(tables, jobs)

	exportWG.Add(1)
	go func(wg *sync.WaitGroup) {
		exportStartEdgesToCSVFile("start.csv", startEdges)
		wg.Done()
	}(&exportWG)

	exportWG.Add(1)
	go func(wg *sync.WaitGroup) {
		exportEndEdgesToCSVFile("end.csv", endEdges)
		wg.Done()
	}(&exportWG)

	inheritEdges := gen.GenerateInhritEdges(tables, jobs, startEdges, endEdges)
	exportInheritEdgesToCSVFile("inherit.csv", inheritEdges)

	exportWG.Wait()
}

func exportTablesToCSVFile(filename string, tables []gen.Table) {
	ifaces := make([]interface{}, len(tables))
	for i := range tables {
		ifaces[i] = tables[i]
	}

	exportToCSVFile(filename, ifaces)
}

func exportJobsToCSVFile(filename string, jobs []gen.Job) {
	ifaces := make([]interface{}, len(jobs))
	for i := range jobs {
		ifaces[i] = jobs[i]
	}

	exportToCSVFile(filename, ifaces)
}

func exportStartEdgesToCSVFile(filename string, edges []gen.StartEdge) {
	ifaces := make([]interface{}, len(edges))
	for i := range edges {
		ifaces[i] = edges[i]
	}

	exportToCSVFile(filename, ifaces)
}

func exportEndEdgesToCSVFile(filename string, edges []gen.EndEdge) {
	ifaces := make([]interface{}, len(edges))
	for i := range edges {
		ifaces[i] = edges[i]
	}

	exportToCSVFile(filename, ifaces)
}

func exportInheritEdgesToCSVFile(filename string, edges []gen.InheritEdge) {
	ifaces := make([]interface{}, len(edges))
	for i := range edges {
		ifaces[i] = edges[i]
	}

	exportToCSVFile(filename, ifaces)
}

func exportToCSVFile(filename string, ifaces []interface{}) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	for _, iface := range ifaces {
		writer.Write(Record(iface))
	}

	writer.Flush()
}

func Record(t interface{}) []string {
	numFields := reflect.ValueOf(t).Elem().NumField()
	record := make([]string, numFields)
	for i := range record {
		record[i] = fmt.Sprintf("%v", reflect.ValueOf(t).Elem().Field(i).Interface())
	}
	return record
}

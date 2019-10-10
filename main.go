package main

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"

	gen "github.com/yixinglu/webank-poc-test-data-generator/generator"
)

func main() {

	clusters := gen.GenerateCluster(gen.ClusterCount)
	users := gen.GenerateUsers(gen.UserCount)
	databases := gen.GenerateDatabases(gen.DatasetCount)
	tables := gen.GenerateTables(gen.DatasetCount, databases, clusters, users)
	jobs := gen.GenerateJobs(gen.JobCount, users)
	startEdges, endEdges := gen.GenerateStartEndEdges(tables, jobs)
	inheritEdges := gen.GenerateInhritEdges(tables, jobs, startEdges, endEdges)

	log.Printf("inherit edges length: %d", len(inheritEdges))

	exportTablesToCSVFile("tables.csv", tables)
	exportJobsToCSVFile("jobs.csv", jobs)
	exportStartEdgesToCSVFile("start.csv", startEdges)
	exportEndEdgesToCSVFile("end.csv", endEdges)
	exportInheritEdgesToCSVFile("inherit.csv", inheritEdges)
}

func Record(t interface{}) []string {
	numFields := reflect.ValueOf(t).Elem().NumField()
	record := make([]string, numFields)
	for i := range record {
		record[i] = fmt.Sprintf("%v", reflect.ValueOf(t).Elem().Field(i).Interface())
	}
	return record
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
	tablecsv, err := os.Create(filename)
	if err != nil {
		log.Fatal(err)
	}

	writer := csv.NewWriter(tablecsv)

	for _, table := range ifaces {
		writer.Write(Record(table))
	}
}

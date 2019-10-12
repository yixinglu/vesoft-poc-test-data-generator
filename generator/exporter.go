package generator

import (
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"reflect"
	"sync"
)

func ExportDatabaseToCSVFile(filename string, databases []Database, wg *sync.WaitGroup) {
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(databases))
		for i := range databases {
			ifaces[i] = databases[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportTablesToCSVFile(filename string, tables []Table, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(tables))
		for i := range tables {
			ifaces[i] = tables[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportJobsToCSVFile(filename string, jobs []Job, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(jobs))
		for i := range jobs {
			ifaces[i] = jobs[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportStartEdgesToCSVFile(filename string, edges []StartEdge, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(edges))
		for i := range edges {
			ifaces[i] = edges[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportEndEdgesToCSVFile(filename string, edges []EndEdge, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(edges))
		for i := range edges {
			ifaces[i] = edges[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportInheritEdgesToCSVFile(filename string, edges []InheritEdge, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(edges))
		for i := range edges {
			ifaces[i] = edges[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportContainEdgesToCSVFile(filename string, edges []ContainEdge, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(edges))
		for i := range edges {
			ifaces[i] = edges[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
}

func ExportReverseContainEdgesToCSVFile(filename string, edges []ReverseContainEdge, wg *sync.WaitGroup) {
	wg.Add(1)

	go func(wg *sync.WaitGroup) {
		ifaces := make([]interface{}, len(edges))
		for i := range edges {
			ifaces[i] = edges[i]
		}

		exportToCSVFile(filename, ifaces)
		wg.Done()
	}(wg)
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
	numFields := reflect.ValueOf(t).NumField()
	record := make([]string, numFields)
	for i := range record {
		record[i] = fmt.Sprintf("%v", reflect.ValueOf(t).Field(i).Interface())
	}
	return record
}

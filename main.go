package main

import (
	"log"

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
}

#!/bin/bash

NEBULA_IMPORTER=nebula-importer-1.0.0-beta.jar
GRAPH_ADDR=127.0.0.1:3699
NAMESPACE=wb

if [ -z "$1" ];then
  NEBULA_IMPORTER=$1
fi

if [ -z "$2" ];then
  GRAPH_ADDR=$2
fi

if [ -z "$3" ];then
  NAMESPACE=$3
fi

java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema job -u user -p password -t vertex --file jobs.csv --column job_id,job_server_ip,hive_user,operation_name,job_type,start_time,end_time
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema tbl -u user -p password -t vertex --file tables.csv --column dataset_id,cluster,table_name,source
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema db -u user -p password -t vertex --file db.csv --column db_id
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema start -u user -p password -t edge --file start.csv --column start_time,end_time
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema end -u user -p password -t edge --file end.csv --column start_time,end_time
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema inherit -u user -p password -t edge --file inherit.csv --column job_id,start_time,end_time
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema contain -u user -p password -t edge --file contain.csv
java -jar $NEBULA_IMPORTER --address $GRAPH_ADDR --name $NAMESPACE --schema reverse_contain -u user -p password -t edge --file reverse_contain.csv

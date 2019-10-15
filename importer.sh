#!/bin/bash

set -exv

NEBULA_IMPORTER=${1:-nebula-importer-1.0.0-beta.jar}
GRAPH_ADDR=${2:-"127.0.0.1:3699"}
NAMESPACE=${3:-wb}

DIR="$(cd "$(dirname "$0")" && pwd)/.csv"

splitfile() {
  CURR_DIR=$DIR/$1
  mkdir -p $CURR_DIR && cd $CURR_DIR
  total_lines=$(wc -l ../$1.csv | cut -f1 -d' ')
  lines=$(($total_lines/10 + 1))

  split -a1 --additional-suffix=.csv -d -l $lines --verbose $DIR/$1.csv $1-
}

parallel() {
  cd $DIR/$1
  TYPE=${2:-vertex}
  for i in {0..9}; do
    java -jar $NEBULA_IMPORTER \
         --address $GRAPH_ADDR \
         --name $NAMESPACE \
         --schema $1 \
         -u user \
         -p password \
         -t $TYPE \
         --file $1-$i.csv \
         --column $3 \
         -d $DIR/$1/err_data_$i.log \
         &
  done
}

cd $DIR
for d in *.csv; do
  dir=${d%.csv}
  splitfile $dir
done

parallel job vertex "job_id,job_server_ip,hive_user,operation_name,job_type,start_time,end_time"
parallel tbl vertex "dataset_id,cluster,table_name,source"
parallel db vertex "db_id"

wait

parallel start edge "start_time,end_time"
parallel end edge "start_time,end_time"
parallel inherit edge "job_id,start_time,end_time"
parallel contain edge ""
parallel reverse_contain edge ""

wait

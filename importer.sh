#!/bin/bash

set -exv

NEBULA_IMPORTER=${1:-nebula-importer-1.0.0-beta.jar}
GRAPH_ADDR=${2:-"127.0.0.1:3699"}
NAMESPACE=${3:-wb}

DIR="$(cd "$(dirname "$0")" && pwd)/.csv"
CONCURRENCY=20

splitfile() {
  CURR_DIR=$DIR/$1
  mkdir -p $CURR_DIR
  pushd $CURR_DIR
  total_lines=$(wc -l ../$1.csv | cut -f1 -d' ')
  lines=$(($total_lines/$CONCURRENCY + 1))
  split -a5 --additional-suffix=.csv -d -l $lines --verbose $DIR/$1.csv $1-
  popd
}

has_failure_data() {
  for ((i=0; i<$CONCURRENCY; i++)); do
    printf -v j "%05d" $i
    if [ -s $1-$j.log ]; then
      echo 1
      break
    fi
  done
  echo 0
}

parallel() {
  CURR_DIR=$1
  SCHEMA=$2
  TYPE=${3:-vertex}
  COLUMNS=$4
  mkdir -p $CURR_DIR/err_data
  for ((i=0;i<$CONCURRENCY;i++)); do
    printf -v j "%05d" $i
    java -jar $NEBULA_IMPORTER \
         --address $GRAPH_ADDR \
         --name $NAMESPACE \
         --schema $SCHEMA \
         -u user \
         -p password \
         -e 5 \
         -t $TYPE \
         --file $CURR_DIR/$SCHEMA-$j.csv \
         -d $CURR_DIR/err_data/$SCHEMA-$j.log \
         --column $COLUMNS \
         &
  done
  wait

  # retry recursely
  # if [ $(has_failure_data $CURR_DIR/err_data/$SCHEMA) -gt 0 ]; then
  #   parallel $CURR_DIR/err_data $SCHEMA $TYPE $COLUMNS
  # fi
}

for d in $DIR/*.csv; do
  base=$(basename $d)
  dir=${base%.csv}
  splitfile $dir
done

parallel $DIR/job job vertex "job_id,job_server_ip,hive_user,operation_name,job_type,start_time,end_time"
parallel $DIR/tbl tbl vertex "dataset_id,cluster,table_name,source"
parallel $DIR/db db vertex "db_id"
parallel $DIR/start start edge "start_time,end_time"
parallel $DIR/end end edge "start_time,end_time"
parallel $DIR/inherit inherit edge "job_id,start_time,end_time"
parallel $DIR/contain contain edge ""
parallel $DIR/reverse_contain reverse_contain edge ""

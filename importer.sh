#!/bin/bash

java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema job -u user -p password -t vertex --file jobs.csv --column job_id,job_server_ip,hive_user,operation_name,job_type,start_time,end_time
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema tbl -u user -p password -t vertex --file tables.csv --column dataset_id,cluster,table_name,source
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema db -u user -p password -t vertex --file db.csv --column db_id
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema start -u user -p password -t edge --file start.csv --column start_time,end_time
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema end -u user -p password -t edge --file end.csv --column start_time,end_time
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema inherit -u user -p password -t edge --file inherit.csv --column job_id,start_time,end_time
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema contain -u user -p password -t edge --file contain.csv
java -jar nebula-importer-1.0.0-beta.jar --address 127.0.0.1:3699 --name webank --schema reverse_contain -u user -p password -t edge --file reverse_contain.csv

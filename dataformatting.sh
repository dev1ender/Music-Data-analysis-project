#!/bin/bash

batchid =`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
LOGFILE =/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid

echo "placing data files from local to HDFS..." >> $LOGFILE


hdfs dfs -rm -r  /user/cloudera/musicProject/batch${batchid}/web/
hdfs dfs -rm -r  /user/cloudera/musicProject/batch${batchid}/formattedweb/
hdfs dfs -rm -r  /user/cloudera/musicProject/batch${batchid}/mob/

hdfs dfs -mkdir -p  /user/cloudera/musicProject/batch${batchid}/web/
hdfs dfs -mkdir -p  /user/cloudera/musicProject/batch${batchid}/mob/

hdfs dfs -put /home/cloudera/musicProject/data/web/* /user/cloudera/Assignment/musicProject/batch${batchid}/web/
hdfs dfs -put /home/cloudera/musicProject/data/mob/* /user/cloudera/Assignment/musicProject/batch${batchid}/mob/

echo "Running pig script for data formatting.." >> $LOGFILE

pig -param batchid=$batchid /home/cloudera/Assignment/musicProject/scripts/dataformatting.pig

echo "Running hive script for formatted data load .. " >> $LOGFILE

hive -hiveconf batchid=$batchid -f /home/cloudera/Assignment/musicProject/scripts/formatted_hive_load.hql




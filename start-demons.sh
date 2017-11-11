#!/bin/bash

if [ -f "/home/cloudera/Assignment/musicProject/logs/current-batch.txt"]
then
	echo "Batch file Found !"
else 
	echo -n "1" > "/home/cloudera/Assignment/musicProject/logs/current-batch.txt"
fi

chmod 775 /home/cloudera/Assignment/musicProject/logs/current-batch.txt
batchid = cat `/home/cloudera/musicProject/logs/current-batch.txt`
LOGFILE = /home/cloudera/Assignment/musicProject/logs/log_batch_$batchid

echo "Starting daemons" >> $LOGFILE

start-all.sh
start-hbase.sh
mr-jobhistory-daemon.sh start historyserver


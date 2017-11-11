batchid =`cat /home/cloudera/Assignment/musicProject/logs/current-batch.txt`
LOGFILE =/home/cloudera/Assignment/musicProject/logs/log_batch_$batchid


echo "Running Spark script for data analysis ..." >>$LOGFILE
echo "exporting analyzed data to local fd..." >>$LOGFILE

cat /home/cloudera/Assignment/musicProject/scripts/data_analysis.scala | spark-shell

echo "All activities completed" >> $LOGFILE

echo "incrementing batchid..." >> $LOGFILE
batchid = `expr $batchid +1`

echo -n $batchid > /home/cloudera/Assignment/logs/current-batch.txt

USE project;

create table if not exists formatted_input
(
User_id STRING,
Song-id STRING,
Artist_id STRING,
Timestamp STRING,
Start_ts STRING,
End_ts STRING,
Geo_cd STRING,
Station_id STRING,
Song_end_type INT,
Like INT,
Dislike INT 
)
Partitoned by(batchid INT)
Row format delimited 
fields terminated by ',';


load Data Inpath '/user/cloudera/musicPorject/batch${hiveconf:batchid}/formattedweb/' Into tables formatted_input partition (batchid=${hiveconf:batchid});


load Data Inpath '/user/cloudera/musicPorject/batch${hiveconf:batchid}/mob/' Into tables formatted_input partition (batchid=${hiveconf:batchid});
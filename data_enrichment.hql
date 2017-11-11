set hive.auto.convert.join=false;


set hive.exec.dynamic.partition.mode=nonstrict;

USE project;

create table if not exist enrichment_data
(
User_id String,
Song_id String,
Artist_id String,
Timestamp String,
Start_ts String,
End_ts String,
Geo_cd String,
Station_id String,
Song_end_type Int.
Like int,
Dislike int
)
partitioned by
(batchid int,
statusString)
stored AS ORC;


Insert overwrite table enriched_data
partition (batchid,status)
select 
i.user_id,
i.song_id,
sa.artist_id,
i.timestamp,
i.start_ts,
i.end_ts,
sg.geo_cd,
i.station_id,
IF (i.song_end_type IS NULL, 3 , i.song_end_type) AS song_end_type,
If (i.like Is Null,0,i.like) AS like,
if (i.dislike if null, 0 ,i.dislike) AS dislike,
i.batchid,
if((i.like=1 AND i.dislike=1)
OR i.user_id IS NULL
OR i.song_id IS NULL
Or i.timestamp IS NULL
or i.start_ts IS  NULL
or i.end_ts IS NULL
or i.geo_cd Is Null
or i.user_id=''
or i.song_id=''
or i.timestamp=''
or i.start_ts=''
or i.end_ts=''
or i.geo_cd=''
or sg.geo_cd IS NULL
or sg.geo_cd IS NULL
or sa.artist_id IS NULL
or sa.artist_id='','fail','pass') as status
from formatted_input i
left outer Join station_geo_map sg on i.station_id = sg.station_id
left outer JOIN song_artist_map sa on I.song_id=sa.song_id
where i.batchid = {hiveconf:batchid};

Insert overwrite local directory '/home/cloudera/Assignment/musicProject/exporteddata/enricheddata'
row format delimited fields terminated by ',' stored as textfile select * from enriched_data;

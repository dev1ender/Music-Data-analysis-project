use project;

create external table if not exists station_geo_map
(
station_id String,
geo_cd String,
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStroagehandler'
with serdeproperties
("hbase.cloumns.mapping"=":key,ger:geo_cd")
tblproperties("hbase.table.name"="station-geo-map");


create external table if not exists subscribed_users
(
user_id String,
subscn_start_dt String,
subscn_end_dt String
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStroagehandler'
with serdeproperties
("hbase.cloumns.mapping"=":key,subscn:startdt,subscn:enddt")
tblproperties("hbase.table.name"="subscribed-users");

Insert overwrite local directory '/home/cloudera/musicProject/exporteddata/subscribeduser' row format delimited
fields terminated by ','
stored as textfile
select * from subscribed_users;

create external table if not exists song-artist_map
(
song_id String,
artist_id String,
)
stored by 'org.apache.hadoop.hive.hbase.HBaseStroagehandler'
with serdeproperties
("hbase.cloumns.mapping"=":key,artist:artistid")
tblproperties("hbase.table.name"="song_artist-map");




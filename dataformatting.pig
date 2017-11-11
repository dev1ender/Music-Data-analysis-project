REGISTER /home/cloudera/Assignment/musicProject/lib/piggybank.jar;

DEFINE XPath org.apache.pig.piggybank.evaluation.xml.XPath();

loadFile = LOAD '/user/cloudera/musicProject/batch${batchid}/web/' using org.apache.pig.piggybank.storage.XMLLoader('record') AS (x:chararray);

Dataforamatting = foreach loadFile generate TRIM(XPath(x,'record/user_id')) AS user_id,
	TRIM(XPath(x,'record/songs_id')) AS song_id,
	TRIM(XPath(x,'record/artist_id')) AS artist_id,
	ToUnixTime(ToDate(TRIM(XPath(x,'record/timestamp')),'yyyy-MM-dd HH:mm:ss')) AS timestamp,
	ToUnixTime(ToDate(TRIM(XPath(x,'record/start_ts')),'yyyy-MM-dd HH:mm:ss')) AS start_ts,
	ToUnixTime(ToDate(TRIM(XPath(x,'record/end_ts')),'yyyy-MM-dd HH:mm:ss')) AS end_ts,
	TRIM(XPath(x,'record/geo_cd')) AS geo_cd,
	TRIM(XPath(x,'record/station_id')) AS station_id,
	TRIM(XPath(x,'record/song_end_type')) AS song_end_type,
	TRIM(XPath(x,'record/like')) AS like,
	TRIM(XPath(x,'record/dislike')) AS dislike;

STORE Dataformatting INTO '/user/cloudera/musicProject/batch${batchid}/formattedweb/' USING PigStorage(',');

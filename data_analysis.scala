import org.apache.spark.sql.Row
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType,StructField,StringType,NumerocType,IntegerType,ArrayType}

import org.apache.spark.sql.fuctions._

//Music Data
val data = sc.textFile("/home/cloudera/Assignment/musicProject/exporteddata/enrichmentdata/000000_")
val MDSchemaString = "user_id:string,song_id:string,artist_id:string,timestamp:string,start_ts:string,end_ts:string,geo_cd:string,station_id:string,song_end_type:string,like:Int,dislike:Int"
val MDdataSchema = StructType(MDSchemaString.split(",").map(fieldinfo => StructField(feildinfo.split(":")(0),if(feildinfo.spit(":")(1).equals("string")) StringType else IntegerType,true)))
val MDrowRDD = data.map(_.split(",")).map(r=>Row(r(0),r(1),r(2),r(3),r(4),r(5),r(6),r(7),r(8).toInt,r(9).toInt,r(10).toInt,r(11).toInt,r(12)))
val MusicDataDF = spark.createDataFrame(MDrowRDD,MDdataSchema)


//subscribed Users
val data sc.textFile("/home/cloudera/Assignment/musicProject/exporteddata/subscribeduser/000000_0")
val SUScehmaString = "user_id:string,start_dt:string,end_dt:string"
val SUdataSchema = StructType(SUScehmaString.split(",").map(fieldinfo => StructField(fieldinfo.split(":")(0),if(fieldinfo.split(":")(1).equals("string")) StringType else IntegerType,true)))
val SUrowRdd = data.map(_.split(",")).map(r=>Row(r(0),r(1),r(2)))
val subscribeduserDF=spark.createDataFrame(SUrowRdd,SUdataSchema)
subscribeduserDF.registerTempTable("Music_SubscribedUsers")

//usesr artist_id
val data = sc.textFile("/home/cloudera/Assignment/musicProject/exporteddata/userartists/000000_0")
val UASchemaString = "user_id:string,artist:string"
val UAdataSchema = StructType(UASchemaString.split(",").map()fieldinfo => StructField(fieldinfo.split(":")(0),if(fieldinfo.split(":")(1).equals("string")) StringType else IntegerType,true)))
val UArowRdd = data.map(_.split(",")).map(r=>Row(r(0),r(1)))
val UserArtistDF = spark.createDataFrame(UArowRdd,UAdataSchema)
UserArtistDF.registerTempTable("Music_UserArtists")

//problem Statement 1
val Top10Stations = spark.sql(s"Select station_id,COUNT(DISTINCT song_id) AS total_distinct_songs_played,COUNT(DISTINCT user_id) AS distinct_user_count,batchid FROM Music_Data WHERE status='pass' AND batchid=$batchid AND like=1 GROUP BY station_id,batchid ORDER BY total_distinct_songs_played DESC LIMIT 10")
Top10Stations.rdd.saveAsTextFile("/home/cloudera/Assignment/musicProject/output/Problem_1_top_10_stations")

//problem statemnet 2
val users_behavior = spark.sql(s"Select CASE WHEN (Subusers.user_id IS NULL OR CAST(music.timestamp AS DECIMAL(20,0)) > CAST(Subusers.end_dt AS DECIMAL(20,0))) THEN 'UNSUBSCRIBED' WHEN (Subusers.user_id IS NOT NULL OR CAST(music.timestamp AS DECIMAL(20,0)) <= CAST(Subusers.end_dt AS DECIMAL(20,0))) THEN 'SUBSCRIBED' END AS user_type,SUM(ABS(CAST(music.end_ts AS DECIMAL(20,)) - CAST(music.start_ts AS DECIMAL(20,0)))) AS duration, batchid FROM Music_Data music LEFT OUTER JOIN Music_SubscribedUsers Subusers ON music.user_id=Subusers.user_id WHERE music.status='pass' AND music.batchid=$batchid GROUP BY user_type,batchid")
users_behavior.rdd.saveAsTextFile("/home/cloudera/Assignment/musicProject/output/Problem_2_Users_behaviour")

//problem 3
val connected_artist = spark.sql(s"SELECT ua.artists,COUNT(DISTINCT ua.user_id) AS user_count,md.batchid FROM Music_UserArtists ua INNER JOIN (SELECT artist_id,song_id,user_id,batchid FROM Music_Data WHERE status='pass' AND batchid=$batchid) md ON ua.artists=md.artist_id AND ua.user_id=md.user_id GROUP BY ua.artists, batchid ORDER BY user_count DESC LIMIT 10")
connected_artist.rdd.saveAsTextFile("/home/cloudera/Assignment/musicProject/output/Problem_3_connected_artists")

//problem 4
val top_10_royalty_songs = spark.sql(s"SELECT song_id,SUM(ABS(CAST(end_ts AS DECIMAL(20,0))-CAST(start_ts AS DECIMAL(20,0)))) AS duration,batchid FROM Music_Data Where status='pass' AND batchid=$batchid AND (like=1 OR song_end_type=0) GROUP BY song_id,batchid ORDER BY duration DESC LIMIT 10")
top_10_royalty_songs.rdd.saveAsTextFile("/home/cloudera/Assignment/musicProject/output/problem_4_top_10_royalty_songs")

//problem  5
Val top_10_unsubscribed_users = spark.sql(s"Select md.user_id,SUM(ABS(CAST(md.end_ts AS DECIMAL(20,0))- CAST(md.start_ts AS DECIMAL(20,0)))) AS duration FROM Music_Data md LEFT OUTER JOIN Music_SubscribedUsers su ON md.user_id WHERE md.status='pass' AND md.batchid=$batchid AND (su.user_id IS NULL OR (CAST(md.timestamp AS DECIMAL(20,0)) > CAST(su.end_dt AS DECIMAL(20,0)))) GROUP BY md.user_id ORDER BY duration DESC LIMIT 10")
top_10_unsubscribed_users.rdd.saveAsTextFile("/home/cloudera/Assignment/musicProject/output/problem_5_top_10_unsubscribed_users")



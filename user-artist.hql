create Database if not exists project;

user project;

create table users_artists(
user_id string,
artist_array Array<STRING>
)
row format delimited
feilds terminated by ','
collection items terminated by '&';

load data local inpath '/home/cloudera/Assignment/musicProject/lookupfiles/user-artist.txt' overwrite into Table user_artists;

Insert overwrite local directory '/home/cloudera/Assignment/musicProject/exporteddata/userartist' row format delimted feilds terminated by ',' stored as textfile select user_id,artists From users_artists lateral view explode(artist_array) a as artists;


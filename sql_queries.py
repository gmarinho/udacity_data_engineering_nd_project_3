import configparser


# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events(
                                artist varchar,
                                auth varchar,
                                firstName varchar,
                                gender char (1),
                                itemInSession int,
                                lastName varchar,
                                length numeric,
                                level varchar,
                                location varchar,
                                method varchar,
                                page varchar,
                                registration numeric,
                                sessionId int,
                                song varchar,
                                status int,
                                ts numeric,
                                userAgent varchar,
                                userId int)
""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs(
                                  artist_id varchar,
                                  artist_latitude numeric,
                                  artist_longitude numeric,
                                  artist_location varchar,
                                  artist_name varchar,
                                  song_id varchar,
                                  title varchar,
                                  duration numeric,
                                  year int)
""")

songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
                    songplay_id bigint IDENTITY(0,1) sortkey,
                    start_time timestamp not null,
                    user_id int not null,
                    level char(4) not null,
                    song_id varchar not null distkey,
                    artist_id varchar not null,
                    session_id int not null,
                    location varchar,
                    user_agent varchar)
""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (
                    user_id int not null sortkey,
                    first_name varchar not null,
                    last_name varchar not null,
                    gender char (1) not null,
                    level char(4) not null)
                    diststyle all;""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (
                    song_id varchar not null sortkey distkey,
                    title varchar not null,
                    artist_id varchar not null,
                    year int not null,
                    duration numeric not null)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (
                    artist_id varchar not null sortkey,
                    name varchar not null,
                    location varchar,
                    latitude numeric,
                    longitude numeric)
                    diststyle all;""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (
                    start_time timestamp primary key,
                    hour int not null,
                    day int not null,
                    week int not null,
                    month int not null,
                    year int not null,
                    weekday int not null)
                    diststyle all;""")

# STAGING TABLES

staging_events_copy = ("""
copy staging_events from {}
credentials 'aws_iam_role={}'
json {};
""").format(config.get('S3', 'LOG_DATA'),
    config.get('IAM_ROLE','ARN'),
    config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
copy staging_songs from {}
credentials 'aws_iam_role={}'
json 'auto'
""").format(config.get('S3', 'SONG_DATA'),
    config.get('IAM_ROLE','ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' as start_time, se.userId, se.level, s.song_id,s.artist_id, se.sessionId, se.location,se.userAgent
FROM staging_events se LEFT JOIN staging_songs s on se.song = s.title AND se.artist =  s.artist_name
WHERE se.page = 'NextSong' AND s.song_id IS NOT NULL
""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT log_max.userId, log_max.firstName, log_max.lastName, log_max.gender, log_max.level
FROM staging_events log_max JOIN
(SELECT max(ts) as ts, userId FROM staging_events
WHERE page = 'NextSong'
GROUP BY (userId)) log_basic
ON log_max.ts = log_basic.ts AND log_max.userId = log_basic.userId
""")

song_table_insert = (""" INSERT INTO songs (song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
""")

artist_table_insert = ("""INSERT INTO artists (artist_id, name, location, latitude, longitude)
SELECT DISTINCT artist_id, artist_name, artist_location,artist_latitude, artist_longitude
FROM staging_songs
""")

time_table_insert = ("""INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT formatted_times.start_time,
EXTRACT(hour from formatted_times.start_time) as hour,
EXTRACT(day from formatted_times.start_time) as day,
EXTRACT(week from formatted_times.start_time) as week,
EXTRACT(month from formatted_times.start_time) as month,
EXTRACT(year from formatted_times.start_time) as year,
EXTRACT(weekday from formatted_times.start_time) as weekday
FROM
(SELECT DISTINCT timestamp 'epoch' + ts/1000 * interval '1 second' as start_time
FROM staging_events
WHERE page = 'NextSong'
) formatted_times
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

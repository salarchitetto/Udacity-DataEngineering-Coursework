import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
    events_id INT IDENTITY(0,1),
    artist VARCHAR,
    auth VARCHAR,
    first_name VARCHAR,
    gender VARCHAR,
    item_in_session INT,
    last_name VARCHAR,
    length NUMERIC,
    user_level VARCHAR,
    location VARCHAR,
    method VARCHAR,
    page VARCHAR,
    registration NUMERIC,
    session_id INT,
    title VARCHAR,
    status INT,
    ts VARCHAR,
    user_agent VARCHAR,
    user_id INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
    num_songs INTEGER,
    artist_id VARCHAR,
    artist_latitude NUMERIC,
    artist_longitude NUMERIC,
    artist_location VARCHAR,
    artist_name VARCHAR,
    song_id VARCHAR,
    title VARCHAR,
    duration NUMERIC,
    year INTEGER)
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) NOT NULL PRIMARY KEY, 
    start_time varchar NOT NULL , 
    user_id int UNIQUE NOT NULL REFERENCES songplays(user_id), 
    level varchar, 
    song_id varchar UNIQUE REFERENCES songplays(song_id), 
    artist_id varchar UNIQUE REFERENCES songplays(artist_id), 
    session_id int , 
    location varchar, 
    user_agent varchar
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id int NOT NULL PRIMARY KEY, 
    first_name varchar NOT NULL, 
    last_name varchar NOT NULL,
    gender varchar NOT NULL, 
    level varchar NOT NULL
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id varchar NOT NULL PRIMARY KEY, 
    title varchar NOT NULL, 
    artist_id varchar NOT NULL, 
    year int, 
    duration numeric
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id varchar NOT NULL PRIMARY KEY, 
    name varchar NOT NULL, 
    location varchar, 
    latitude numeric, 
    longitude numeric
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP NOT NULL PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month varchar, 
    year int, 
    weekday varchar
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    compupdate off 
    format as JSON {}
""").format(config.get('S3', 'LOG_DATA'),
            config.get('IAM_ROLE', 'ARN'),
            config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' 
    compupdate off
    JSON 'auto'
    """).format(config.get('S3','SONG_DATA'), 
                config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, 
                            artist_id, session_id, location, user_agent) 
    SELECT DISTINCT 
        events.ts, 
        events.user_id, 
        events.user_level,
        songs.song_id,
        songs.artist_id,
        events.session_id,
        events.location,
        events.user_agent
    FROM staging_events as events, staging_songs as songs
    WHERE events.page = 'NextSong'
    AND events.title = songs.title
    AND user_id NOT IN (SELECT DISTINCT songs.user_id FROM songplays as songs WHERE songs.user_id = user_id
                       AND songs.start_time = start_time AND songs.session_id = session_id )
""")
user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    
    SELECT DISTINCT
        events.user_id,
        events.first_name,
        events.last_name,
        events.gender,
        events.user_level
    FROM staging_events as events
    WHERE events.page = 'NextSong'
    AND events.user_id NOT IN (SELECT DISTINCT user_id FROM users)
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    
    SELECT DISTINCT 
        songs.song_id,
        songs.title,
        songs.artist_id,
        songs.year,
        songs.duration
    FROM staging_songs as songs
    WHERE songs.song_id NOT IN (SELECT DISTINCT song_id FROM songs)
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    
    SELECT DISTINCT 
        songs.artist_id, 
        songs.title,
        songs.artist_location,
        songs.artist_latitude,
        songs.artist_longitude
    FROM staging_songs as songs
    WHERE songs.artist_id NOT IN (SELECT DISTINCT artist_id FROM artists)
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    
    SELECT 
        start_time,
        EXTRACT(hr from start_time) AS hour,
        EXTRACT(d from start_time) AS day,
        EXTRACT(w from start_time) AS week,
        EXTRACT(mon from start_time) AS month,
        EXTRACT(yr from start_time) AS year, 
        EXTRACT(weekday from start_time) AS weekday 
    FROM (
        SELECT DISTINCT  TIMESTAMP 'epoch' + ts/1000 *INTERVAL '1 second' as start_time 
        FROM staging_events s     
    )
    WHERE start_time NOT IN (SELECT DISTINCT start_time FROM time)
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

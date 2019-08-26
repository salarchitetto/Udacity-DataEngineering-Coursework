# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id serial NOT NULL PRIMARY KEY, 
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
    start_time bigint NOT NULL PRIMARY KEY, 
    hour int, 
    day int, 
    week int, 
    month varchar, 
    year int, 
    weekday varchar
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays (start_time, user_id, level, song_id, artist_id,
    session_id, location, user_agent) 
    VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT DO NOTHING;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name,
    gender, level)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (user_id) DO UPDATE 
    SET user_id = excluded.user_id,
        first_name = excluded.first_name,
        last_name = excluded.last_name,
        gender = excluded.gender,
        level = excluded.level;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id,
    year, duration)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location,
    latitude, longitude)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time (start_time, hour, day,
    week, month, year, weekday)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT 
    s.song_id, 
    a.artist_id 
    FROM songs AS s
    LEFT OUTER JOIN artists AS a 
        ON s.artist_id = a.artist_id
    WHERE s.title = %s AND a.name = %s AND s.duration = %s;
""")

# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, 
                        artist_table_create, time_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, 
                      artist_table_drop, time_table_drop]

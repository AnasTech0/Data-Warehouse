import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar NOT NULL,
        auth varchar,
        firstName varchar,
        gender char,
        itemInSession int,
        lastName varchar,
        length float,
        level varchar,
        location varchar,
        method varchar,
        page varchar,
        registration float,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent varchar,
        userId varchar);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int,
        artist_id varchar,
        latitude float,
        longitude float,
        location varchar,
        name varchar,
        song_id varchar,
        title varchar,
        duration float,
        year int);
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplay( 
        songplay_id int identity(0,1) PRIMARY KEY,
        start_time timestamp SORTKEY,
        user_id int NOT NULL,
        level text NOT NULL,
        song_id int DISTKEY,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar
        );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user(
        user_id int NOT NULL,
        first_name text NOT NULL,
        last_name text SORTKEY,
        gender char NOT NULL,
        level text NOT NULL
        );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song(
        song_id varchar NOT NULL,
        title varchar NOT NULL,
        artist_id varchar,
        year int SORTKEY,
        duration numeric NOT NULL
        );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist(
        artist_id varchar NOT NULL,
        name text NOT NULL SORTKEY,
        latitude numeric,
        longitude numeric            
        )distyle all;
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time(
        start_time timestamp NOT NULL SORTKEY,
        hour int,
        day int,
        month int,
        year int,
        weekday int 
        )distyle all;
""")

# STAGING TABLES

staging_events_copy = ("""
        copy staging_events from {} IAM_ROLE '{}' region 'us-east-1' JSON{}
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'], config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
        copy staging_songs from {} IAM_ROLE '{}' region 'us-east-1' JSON 'auto'
""").format(config['S3']['LOG_DATA'], config['IAM_ROLE']['ARN'])

# FINAL TABLES

songplay_table_insert = ("""
        INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
        SELECT DISTINCT timestamp 'epoch' + ts / 1000 * interval '1 second' AS start_time,
            user_id,
            level,
            song_id,
            artist_id,
            session_id,
            location,
            user_agent
        FROM staging_events, staging_songs
        WHERE staging_events.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO user(userId, first_name, last_name, gender, level)
    SELECT DISTINCT userId, first_name, last_name, gender, level from staging_events
    WHERE userId is NOT NULL AND 
    first_name is NOT NULL AND 
    gender is NOT NULL AND
    level is NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO song(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id, title, artist_id, year, duration from staging_songs
    WHERE song_id is NOT NULL AND
    title is NOT NULL AND
    duration is NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artist(artist_id, name, latitude, longitude)
    SELECT DISTINCT artist_id, name, latitude, longitude from staging_songs
    WHERE artist_id is NOT NULL AND name is NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT 
            TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' AS start_time,
            EXTRACT(hour FROM start_time) AS hour,
            EXTRACT(day FROM start_time) AS day,
            EXTRACT(week FROM start_time) AS week,
            EXTRACT(month FROM start_time) AS month,
            EXTRACT(year FROM start_time) AS year,
            EXTRACT(weekday FROM start_time) AS weekday
    FROM staging_events
        WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

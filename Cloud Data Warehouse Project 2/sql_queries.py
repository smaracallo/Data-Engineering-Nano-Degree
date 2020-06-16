import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop =  "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop =       "DROP TABLE IF EXISTS songplays"
user_table_drop =           "DROP TABLE IF EXISTS users"
song_table_drop =           "DROP TABLE IF EXISTS song"
artist_table_drop =         "DROP TABLE IF EXISTS artist"
time_table_drop =           "DROP TABLE IF EXISTS time"

# CREATE TABLES

# Staging Tables
staging_events_table_create= ("""
    CREATE TABLE staging_events(
        artist              VARCHAR,
        auth                VARCHAR,
        firstName           VARCHAR,
        gender              VARCHAR,
        itemInSession       INT,
        lastName            VARCHAR,
        length              FLOAT,
        level               VARCHAR,
        location            VARCHAR,
        method              VARCHAR,
        page                VARCHAR,
        registration        FLOAT,
        sessionId           INT,
        song                VARCHAR,
        status              INT,
        ts                  TIMESTAMP,
        userAgent           VARCHAR,
        userId              INT
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs(
        num_songs           INT,
        artist_id           VARCHAR,
        artist_latitude     FLOAT,
        artist_longitude    FLOAT,
        artist_location     VARCHAR,
        artist_name         VARCHAR,
        song_id             VARCHAR,
        title               VARCHAR,
        duration            FLOAT,
        year                INT
    )
""")


# Fact Table

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT        IDENTITY(0,1) NOT NULL PRIMARY KEY, 
    start_time  TIMESTAMP                NOT NULL, 
    user_id     INT                      NOT NULL, 
    level       varchar, 
    song_id     varchar                  NOT NULL, 
    artist_id   varchar                  NOT NULL, 
    session_id  INT, 
    location    varchar, 
    user_agent  TEXT);
""")


# Dimension Tables

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
    user_id     INT       PRIMARY KEY, 
    first_name  varchar, 
    last_name   varchar, 
    gender      varchar, 
    level       varchar);
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
    song_id   VARCHAR PRIMARY KEY,
    title     VARCHAR,
    artist_id VARCHAR,
    year      INT,
    duration  FLOAT);
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR PRIMARY KEY, 
    name      VARCHAR, 
    location  VARCHAR, 
    latitude  FLOAT, 
    longitude FLOAT);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY, 
    hour       INT, 
    day        INT, 
    week       INT, 
    month      INT, 
    year       INT, 
    weekday    INT);
""")


# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events from {data_bucket}
    credentials 'aws_access_key_id={key};aws_secret_access_key={secret}'
    region 'us-west-2' format AS JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], key=config['AWS']['KEY'], secret=config['AWS']['SECRET'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    COPY staging_songs from {data_bucket}
    credentials 'aws_access_key_id={key};aws_secret_access_key={secret}'
    region 'us-west-2' format as JSON 'auto';
""").format(data_bucket=config['S3']['LOG_DATA'], key=config['AWS']['KEY'], secret=config['AWS']['SECRET'])


# FINAL TABLES INSERTS

songplay_table_insert = ("""
   INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT DISTINCT to_timestamp(to_char(se.ts, '9999-99-99 99:99:99'),'YYYY-MM-DD HH24:MI:SS'),
                se.userId    AS user_id,
                se.level     AS level,
                ss.song_id   AS song_id,
                ss.artist_id AS artist_id,
                se.sessionId AS session_id,
                se.location  AS location,
                se.userAgent AS user_agent
    FROM staging_events se
    JOIN staging_songs ss ON se.song = ss.title AND se.artist = ss.artist_name;
""")

user_table_insert = ("""
    INSERT INTO users (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT(userId) AS user_id,
                firstName   AS first_name,
                lastName    AS last_name,
                gender      AS gender,
                level       AS level
    FROM staging_events
    where userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (song_id, title, artist_id, year, duration)
    SELECT  DISTINCT(song_id) AS song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists (artist_id, name, location, latitude, longitude)
    SELECT  DISTINCT(artist_id) AS artist_id,
            artist_name         AS name,
            artist_location     AS location,
            artist_latitude     AS latitude,
            artist_longitude    AS longitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time (start_time, hour, day, week, month, year, weekday)
    SELECT  DISTINCT(start_time)                AS start_time,
            EXTRACT(hour FROM start_time)       AS hour,
            EXTRACT(day FROM start_time)        AS day,
            EXTRACT(week FROM start_time)       AS week,
            EXTRACT(month FROM start_time)      AS month,
            EXTRACT(year FROM start_time)       AS year,
            EXTRACT(dayofweek FROM start_time)  AS weekday
    FROM songplays;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]

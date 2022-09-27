import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS staging_events
    (
        artist varchar,
        auth varchar,
        firstName varchar,
        gender char(1),
        itemInSession smallint,
        lastName varchar,
        length float,
        level char(10),
        location text,
        method char(10),
        page varchar,
        registration float,
        sessionId int,
        song varchar,
        status int,
        ts bigint,
        userAgent text,
        userId int
    )
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS staging_songs
    (
        num_songs int,
        artist_id varchar,
        artist_latitude float,
        artist_longitude float,
        artist_location text,
        artist_name text,
        song_id varchar,
        title text,
        duration float,
        year int
    )
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id int identity(0,1) primary key,
        start_time timestamp not null sortkey distkey,
        user_id int not null,
        song_id char(50),
        artist_id char(50),
        session_id int,
        level char(10),
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int primary key sortkey,
        first_name varchar,
        last_name varchar,
        gender char(2),
        level char(10)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id char(50) primary key sortkey,
        title varchar not null,
        artist_id char(50),
        year int,
        duration float not null
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id char(50) primary key sortkey,
        name varchar not null, 
        location varchar,
        latitude float,
        longtitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp primary key sortkey distkey,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday text
    )
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_events FROM {} 
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS JSON {}
    COMPUPDATE OFF
    TIMEFORMAT as 'epochmillisecs';
    """).format(LOG_DATA, ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    copy staging_songs from {}
    credentials 'aws_iam_role={}'
    region 'us-west-2' format as JSON 'auto';
""").format(SONG_DATA, ARN)

# FINAL TABLES
songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, song_id, artist_id, session_id, level, location, user_agent) 
    SELECT DISTINCT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
            e.userId as user_id,
            s.song_id as song_id,
            s.artist_id as artist_id,
            e.sessionId as session_id,
            e.level as level,
            e.location as location,
            e.userAgent as user_agent
    FROM staging_events e 
    JOIN staging_songs s ON (e.song = s.title AND e.length = s.duration AND e.artist = s.artist_name)
    AND e.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userId as user_id,
            firstName as first_name,
            lastName as last_name,
            gender,
            level
    FROM staging_events        
    WHERE page = 'NextSong' AND userId IS NOT NULL;
""")


song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
            title,
            artist_id,
            year,
            duration
    FROM staging_songs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longtitude)
    SELECT DISTINCT artist_id,
            artist_name as name,
            artist_location as location,
            artist_latitude as latitude,
            artist_longitude as longtitude
    FROM staging_songs
    WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * INTERVAL '1 second' as start_time,
            EXTRACT(hour FROM start_time),
            EXTRACT(day FROM start_time),
            EXTRACT(week FROM start_time),
            EXTRACT(month FROM start_time),
            EXTRACT(year FROM start_time),
            EXTRACT(weekday FROM start_time)
    FROM staging_events;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
# insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]


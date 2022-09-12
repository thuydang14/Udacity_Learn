# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplay CASCADE"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE"

# CREATE TABLES

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS songplays
    (
        songplay_id serial primary key,
        start_time timestamp references time(start_time) not null,
        user_id int references users(user_id) not null,
        song_id char(20) references songs(song_id),
        artist_id char(20) references artists(artist_id),
        session_id int,
        level char(10),
        location text,
        user_agent text
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users
    (
        user_id int primary key,
        first_name varchar,
        last_name varchar,
        gender char(2),
        level char(10)
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs
    (
        song_id char(20) primary key,
        title varchar not null,
        artist_id char(20) references artists(artist_id),
        year int,
        duration float not null
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists
    (
        artist_id char(20) primary key,
        name varchar not null, 
        location varchar,
        latitude float,
        longtitude float
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time
    (
        start_time timestamp primary key,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday text
    )
""")

# INSERT RECORDS

songplay_table_insert = ("""
    INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent) 
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

user_table_insert = ("""
    INSERT INTO users(user_id, first_name, last_name, gender, level) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(user_id) DO UPDATE SET level = EXCLUDED.level;
""")

song_table_insert = ("""
    INSERT INTO songs(song_id, title, artist_id, year, duration) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(song_id) DO NOTHING;
""")

artist_table_insert = ("""
    INSERT INTO artists(artist_id, name, location, latitude, longtitude) 
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT(artist_id) DO NOTHING;
""")


time_table_insert = ("""
    INSERT INTO time(start_time, hour, day, week, month, year, weekday) 
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT(start_time) DO NOTHING;
""")

# FIND SONGS

song_select = ("""
    SELECT songs.song_id, artists.artist_id 
    FROM songs JOIN artists ON songs.artist_id = artists.artist_id
    WHERE songs.title=%s AND artists.name=%s AND songs.duration=%s
""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create, song_table_create, time_table_create, songplay_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
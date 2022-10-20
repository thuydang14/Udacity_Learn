import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

ARN = config['IAM_ROLE']['ARN']
VIDEO = config['S3']['VIDEO']
CATEGORY = config['S3']['CATEGORY']
YOUTUBE_TREND = config['S3']['CATEGORY'] 


# DROP TABLES
fact_youtube_trend_drop = "DROP TABLE IF EXISTS fact_youtube_trend"
dim_video_drop = "DROP TABLE IF EXISTS dim_video"
dim_category_drop = "DROP TABLE IF EXISTS dim_category"



# CREATE TABLES
staging_category_create_table = ("""
    CREATE TABLE IF NOT EXISTS staging_category
    (
        etag varchar(1000),
        id varchar,
        assignable boolean,
        channelId varchar,
        title var(500)
    )
""")

staging_video_create_table = ("""
    CREATE TABLE IF NOT EXISTS staging_video
    (
        video_id varchar(500),
        trending_date date,
        title 
    )
""")


fact_youtube_trend_table_create = ("""
    CREATE TABLE IF NOT EXISTS fact_youtube_trend
    (
        id int identity(0,1) primary key,
        video_id varchar references dim_video(video_id) not null,
        trending_date date not null distkey,
        category_id int references dim_category(category_id) not null,
        publish_time timestamp sortkey not null,
        views int,
        likes int,
        dislikes int,
        comment_count int
    )
""")

dim_category_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_category
    (
        category_id int primary key,
        title varchar
    )
""")

dim_video_table_create = ("""
    CREATE TABLE IF NOT EXISTS dim_video
    (
        video_id varchar primary key,
        title varchar(65535) not null,
        channel_title varchar(65535) not null,
        tags varchar(65535),
        comments_disabled boolean,
        ratings_disabled boolean,
        video_error_or_removed boolean
    )
""")


# STAGING TABLE
copy_to_video_table= ("""
    COPY dim_video
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS PARQUET;
""").format(VIDEO, ARN)

copy_to_category_table = ("""
    COPY dim_category
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS PARQUET;
""").format(CATEGORY, ARN)

copy_to_fact_table = ("""
    COPY fact_youtube_trend
    FROM {}
    CREDENTIALS 'aws_iam_role={}'
    REGION 'us-west-2'
    FORMAT AS PARQUET;
""").format(YOUTUBE_TREND, ARN)



# FINAL TABLES
fact_youtube_trend_table_insert = ("""
    INSERT INTO fact_youtube_trend(video_id, trending_date, category_id, publish_time, views, likes, dislikes, comment_count)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
""")

dim_video_table_insert = ("""
    INSERT INTO dim_video(video_id, title, channel_title, tags, comments_disabled, ratings_disabled, video_error_or_removed)
    VALUES (%s, %s, %s, %s, %s, %s, %s);
""")

dim_category_table_insert = ("""
    INSERT INTO dim_category(category_id, title)
    VALUES (%s, %s);
""")


#CHECK QUALITY
fact_table_check = ("""
    SELECT COUNT(*) FROM fact_youtube_trend;
""")

dim_video_check = ("""
    SELECT COUNT(*) FROM dim_video;
""")

dim_category_check = ("""
    SELECT COUNT(*) FROM dim_category;
""")


#ON CONFLICT DO NOTHING
# QUERY LISTS
create_table_queries = [dim_category_table_create, dim_video_table_create, fact_youtube_trend_table_create]
drop_table_queries = [fact_youtube_trend_drop, dim_video_drop, dim_category_drop]
insert_table_queries = [dim_category_table_insert, dim_video_table_insert, fact_youtube_trend_table_insert]
copy_table_queries = [copy_to_category_table, copy_to_video_table, copy_to_fact_table]
quality_check_queries = [dim_video_check, dim_category_check, fact_table_check]
# insert_table_queries = [user_table_insert, artist_table_insert, song_table_insert, time_table_insert, songplay_table_insert]


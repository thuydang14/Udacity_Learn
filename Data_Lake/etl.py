import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType, StructType, StructField, DoubleType, StringType, IntegerType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """
        Description: this function use to create sparksession
        Arguments: None
        Returns: None
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    """
    Description: This function use to process song data from raw file in filepath directory, process and store data in
    parquet format then store in S3 bucket
    
    Arguments:
        spark: the spark session
        input_data: input data from S3 bucket of udacity.
        output_data: output data to my own S3 bucket.
    
    return: None
    """
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*.json'
    
    song_schema = StructType([
        StructField("artist_id", StringType()),
        StructField("artist_latitude", DoubleType()),
        StructField("artist_location", StringType()),
        StructField("artist_longitude", DoubleType()),
        StructField("artist_name", StringType()),
        StructField("duration", DoubleType()),
        StructField("num_songs", IntegerType()),
        StructField("song_id", StringType()),
        StructField("title", StringType()),
        StructField("year", IntegerType()),
    ])
    # read song data file
    df_song = spark.read.json(song_data, schema = song_schema)

    # extract columns to create songs table
    songs_table = df_song.select(['song_id', 'title', 'artist_id', 'year', 'duration'])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.dropDuplicates(['song_id']).write.partitionBy("year", "artist_id").mode("overwrite").parquet(path=output_data+'songs')

    # extract columns to create artists table
    artists_table = df_song.selectExpr(['artist_id', 'artist_name as name', 'artist_location as location', 'artist_latitude as latitude', 'artist_longitude as longitude'])
    
    # write artists table to parquet files
    artists_table.dropDuplicates(['artist_id']).write.mode("overwrite").parquet(path=output_data+'artists')


def process_log_data(spark, input_data, output_data):
    """
    Description: This function use to process log data from raw file in filepath directory, process and store data in
    parquet format then store in S3 bucket
    
    Arguments:
        spark: the spark session
        input_data: input data from S3 bucket of udacity.
        output_data: output data to my own S3 bucket.
    
    return: None
    """
    # get filepath to log data file
    log_data = input_data + 'log_data/*.json'

    # read log data file
    df_log = spark.read.json(log_data)
    
    # filter by actions for song plays
    df_log = df_log.filter(df_log.page == "NextSong")

    # extract columns for users table    
    users_table = df_log.selectExpr(['userId as user_id', 'firstName as first_name', 'lastName as last_name', 'gender', 'level'])
    
    # write users table to parquet files
    users_table.dropDuplicates().write.mode("overwrite").parquet(path=output_data+'users')

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x/1000.0), TimestampType())
    df_log = df_log.withColumn("timestamp", get_timestamp(df_log.ts))
    # create datetime column from original timestamp column
    #     get_datetime = udf()
    #     df = 
    df_log=df_log.withColumn('hour', hour(df_log.timestamp))\
            .withColumn('day', dayofmonth(df_log.timestamp))\
            .withColumn('week', weekofyear(df_log.timestamp))\
            .withColumn('month', month(df_log.timestamp))\
            .withColumn('year', year(df_log.timestamp))\
            .withColumn('weekday', dayofweek(df_log.timestamp))
    
    # extract columns to create time table
    time_table = df_log.selectExpr(['timestamp as start_time','hour', 'day', 'week','month', 'year', 'weekday'])
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(path=output_data+'time')

    # read in song data to use for songplays table
    song_path = input_data + 'song_data/*/*/*/*.json'
    df_song = spark.read.json(song_path)
    
    # join dataframe song_df to log dataframe(df)
    relation = [df_log.artist == df_song.artist_name, df_log.song == df_song.title]
    songplays = df_log.join(df_song.drop('year'), relation, 'inner')
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songplays.selectExpr(['timestamp as start_time',
                          'userId as user_id',
                          'level',
                          'song_id',
                          'artist_id',
                          'sessionId as session_id',
                          'location',
                          'userAgent as user_agent',
                          'month',
                          'year'])

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(path=output_data+'songplays')


def main():
    """
        Description: main function to process all funtion above 
        Arguments: None
        Returns: None
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://thuysparkify/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()

import configparser
import psycopg2
from sql_queries import *
import os
from schema import schema_video, schema_category
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType, ArrayType

config = configparser.ConfigParser()
config.read('dwh.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['KEY']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['SECRET']
        
def create_spark_session():
    spark = SparkSession.builder.\
        config("spark.jars.repositories", "https://repos.spark-packages.org/").\
        config("spark.jars.packages", "saurfang:spark-sas7bdat:2.0.0-s_2.11").\
        config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0").\
        enableHiveSupport().getOrCreate()
    return spark
    
    
def process_category_file(spark, filepath, output_data):
    """
    Description: This function will read and process category data from youtube_data source
    folder by spark then write to PostgreSQL hosted on AWS Redshift
    Arguments:
        cur: the cursor object
        conn: connection to postgresql using psycopg2.
        spark: the SparkSession
        filepath: path of files    
    
    return: None
    """
    #Read file JSON
    df_category = spark.read\
                .option('multiline', 'true')\
                .schema(schema_category)\
                .json(filepath)
    
    #Explode data in items field
    explode_category = df_category.select(F.explode('items').alias('category')).select('category.*')
    
    #process data
    dim_category = explode_category.select('etag', 'id', 'snippet.assignable', 'snippet.channelId', 'snippet.title')
    dim_category = dim_category.dropDuplicates(['id', 'title'])
    dim_category = dim_category.select(['id', 'title']).sort('id', ascending=True)
    
    #write dataframe to parquet file and stage in S3 bucket
    dim_category.write.mode("overwrite").parquet(path=output_data+'category')
    

    
    
def process_video_file(spark, filepath, output_data):
    """
    Description: This function will read and process video data from youtube_data source
    folder by spark then write to PostgreSQL hosted on AWS Redshift
    Arguments:
        cur: the cursor object
        conn: connection to postgresql using psycopg2.
        spark: the SparkSession
        filepath: path of files    
    
    return: None
    """
    df_video = spark.read.format("csv")\
            .option("header", True)\
            .option("multiLine", True)\
            .option("delimiter", ",")\
            .schema(schema_video)\
            .load(filepath)
    
    #Process data video file
    df_video = df_video.dropDuplicates(['video_id'])
    df_video = df_video.drop(F.col('thumbnail_link'))
    df_video = df_video.withColumn('trending_date', F.to_date(F.col('trending_date'), 'yy.dd.MM').alias('date'))
    tags_to_list = F.udf(lambda x: x.replace('"' ,'').split('|'))
    df_video = df_video.withColumn('tags', F.when(F.col('tags') != '[none]',
                                        tags_to_list(F.col('tags')))
                                        .otherwise('[]'))
    
    #Create dimension video table dataframe
    dim_video = df_video.select(['video_id', 
                                 'title', 
                                 'channel_title', 
                                 'tags', 
                                 'comments_disabled', 
                                 'ratings_disabled', 
                                 'video_error_or_removed'])
    
    #write dataframe to parquet file and stage in S3 bucket
    dim_video.write.mode("overwrite").parquet(path=output_data+'video')
    
    #Create fact table dataframe
    fact_youtube_trending = df_video.select(['video_id', 
                                             'trending_date', 
                                             'category_id', 
                                             'publish_time', 
                                             'views', 
                                             'likes', 
                                             'dislikes', 
                                             'comment_count'])
    
    #write dataframe to parquet file and stage in S3 bucket
    fact_youtube_trending.write.partitionBy("trending_date").mode("overwrite").parquet(path=output_data+'youtube_trend')
    
def load_data_to_table(cur, conn):
    """
    Description: This function will insert data directly from S3 bucket to table.
    
    Arguments:
        cur: the cursor object
        conn: connection to postgresql using psycopg2
    
    return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()
    
    
def quality_check(cur):
    """
        Description: this function will execute query to check data 
        inserted to table.
        Arguments:
            cur: the cursor object
        Returns: None
    """
    for query in quality_check_queries:
        cur.execute(query)
        response = cur.fetchone()
        if response[0] <= 0:
            return 'Quality check failed !!!!'
        else: 
            return 'There have data. Pipeline success write data'

def main():
    """
        Description: main function will read config from dwh.cfg file and connect to redshift cluster,
        then load data from local and process data then write data to parquet file and stage in S3 
        after will copy data to youtubetrend database hosted by Redshift.
        Arguments: None
        Returns: None
    """
    
    spark = create_spark_session()
    
    category = './youtube_data/category/*.json'
    video = './youtube_data/video/*.csv'
    
    s3 = 's3a://youtubetrending/'
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    
    process_category_file(spark, category, s3)
    process_video_file(spark, video, s3)
    load_data_to_table(cur, conn)
    
    quality_check(cur)
    
    
    conn.close()


if __name__ == "__main__":
    main()
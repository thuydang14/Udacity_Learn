# CAPSTONE PROJECT: Youtube Trending

## Project Tree
.
├── cluster_implement.ipynb
├── create_tables.py
├── dwh.cfg
├── etl.py
├── __pycache__
│   ├── schema.cpython-36.pyc
│   └── sql_queries.cpython-36.pyc
├── schema.py
├── sql_queries.py
├── tranform.ipynb
└── youtube_data
    ├── category
    │   ├── CA_category_id.json
    │   ├── DE_category_id.json
    │   ├── FR_category_id.json
    │   ├── GB_category_id.json
    │   ├── IN_category_id.json
    │   ├── JP_category_id.json
    │   ├── KR_category_id.json
    │   ├── MX_category_id.json
    │   ├── RU_category_id.json
    │   └── US_category_id.json
    └── video
        ├── CAvideos.csv
        ├── DEvideos.csv
        ├── FRvideos.csv
        ├── GBvideos.csv
        ├── INvideos.csv
        ├── JPvideos.csv
        ├── KRvideos.csv
        ├── MXvideos.csv
        ├── RUvideos.csv
        └── USvideos.csv
        
        
## Project Explanation

This project will help Data Analytics team to analys trending video on youtube, based on tag, view, like, dislike, etc... 
to help content creator startup company will have an closer look in what are trending on youtube.
Data for project are taken from KAGGLE dataset, store in local at raw format.
Using Pyspark to process data and write to Parquet format then store in S3 bucket.
After store in S3 bucket, data will be copy to Youtube Trend database hosted by AWS Redshift.
Whenever Copy operation is completed, an quality check function will be execute and check whether any data inserted to database.


## Schema
### Category
   category_id int
   title varchar
 
 
### Video
   video_id varchar primary key,
   title varchar(65535) not null,
   channel_title varchar(65535) not null,
   tags varchar(65535),
   comments_disabled boolean,
   ratings_disabled boolean,
   video_error_or_removed boolean
   

### Youtube Trending
   id int identity(0,1) primary key,
   video_id varchar,
   trending_date date,
   category_id int
   publish_time timestamp,
   views int,
   likes int,
   dislikes int,
   comment_count int
   
   
## Build ETL Pipeline
1. Run *Cluster_implement.ipynb* to create Redshift cluster, S3 bucket, and IAM role for access to S3
2. Run *create_table.py* to create table on Redshift cluster
3. Run *etl.py* to execute ETL process
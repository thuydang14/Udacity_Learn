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

This project will help Data Analytics team to analys trending videos on youtube, based on tag, view, like, dislike, etc... 
to help content creator startup company will have an closer look in what are trending on youtube.
Data for project are taken from KAGGLE dataset, store in local at raw format.
Using Pyspark to process data and write to Parquet format then store in S3 bucket.
After store in S3 bucket, data will be copy to Youtube Trend database hosted by AWS Redshift.
Whenever Copy operation is completed, an quality check function will be execute and check whether any data inserted to database.


## Technologies

### Spark Dataframe

In this project I use Spark framework to process data, because it very useful for process bigdata.
To compare the processing data operation in Spark and Pandas, in which Pandas run operation on simple machine whereas 
Pyspark run on multiple machines. It's helpful to deal with very large data.

### Star Schema

The schema used in this project is Star Schema, because there will be a lot of data be inserted to fact table, fact table will store 
measured data and dimention tables will store attribute about data. To be compare with Snowflake Schema our fact table may be have
some redundant data which that not be accepted in Snowflake Schema, and the critical reason here is our database just have 2 relational.

### Entity connect

Our database have 2 entity are video and category, and we have some attribute is measured, so we create 3 table that named:
1. Fact table: fact_youtube_trend
2. Dimension table: dim_video, dim_category

We will connect two ID field in fact table(video_id, category_id) to dimentional tables:
Connection_1: fact_youtube_trend(video_id) -> dim_video(video_id) relation: 1-N
Connection_2: fact_youtube_trend(category_id) -> dim_category(category_id) relation: 1-N

The connection will help we can retrieve the detail information about video follow id in query statement


## Data should be updated oftenly

Because this is an trending database for youtube, and trend is updated everyday and every hour.
So it is necessary to update data everyday that help startup content creator company to be update what is trending.


## Data Scalable

Because of data will be updated every day, data will become larger everyday and data will be accessed by more than 
100 people for using these data to analytic varies case.

## Database will increase three scenarios:
1. The data was increased by 100x.
2. The pipelines would be run on a daily basis by 7 am every day.
3. The database needed to be accessed by 100+ people.
### Answer:
- The answear for question 1 and 3 is use AWS EMR instead of Redshift cluster because it will take lower cost than increase 
node on Redshift, and AWS EMR also easy to scale up with Elastic attribute. EMR also easier for increase number of user
accesse to cluster with Amazon EMR User Role Mapper, help admin to manage the accessible for specific user more effectively
- We will use Apache Airflow in the future to run the pipeline daily basis by 7 am everyday, with Airflow you can define your pipeline step very easy to understand with DAG.





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

## Data Dictionary
### Dim_category table

| Column      | Description                 |
|-------------|-----------------------------|
| category_id | ID of category, Primary key |
| title       | Name of category            |

### Dim_video table

| Column                 | Description                                       |
|------------------------|---------------------------------------------------|
| video_id               | ID of video on youtube                            |
| title                  | Title of video                                    |
| channel_title          | Title of channel which post this video allow null |
| tags                   | Tag of video                                      |
| comments_disabled      | This video can comment (Yes/No)                   |
| ratings_disabled       | This video can rating (Yes/No)                    |
| video_error_or_removed | This video removed (Yes/No)                       |

### Fact_youtube_trend table

| Column        | Description                                                                       | Foreign key               |
|---------------|-----------------------------------------------------------------------------------|---------------------------|
| id            | ID for fact table, this is increment field and is primary key                     |                           |
| video_id      | ID of video on youtube, this field reference to field video_id on dim_video table | dim_video(video_id)       |
| trending_date | The date that video on top trending                                               |                           |
| category_id   | ID of category, this field reference to field category_id on dim_category table   | dim_category(category_id) |
| publish_time  | The time that video being published                                               |                           |
| views         | Total views of video                                                              |                           |
| likes         | Total likes of video                                                              |                           |
| dislikes      | Total dislikes of video                                                           |                           |
| comment_count | Total comment of video                                                            |                           |

## Build ETL Pipeline

1. Run *Cluster_implement.ipynb* to create Redshift cluster, S3 bucket, and IAM role for access to S3
2. Run *create_table.py* to create table on Redshift cluster
3. Run *etl.py* to execute ETL process


## Result

### Top 10 trending video.

- Code

select distinct(f.video_id), v.title, c.title, f.likes
from fact_youtube_trend f join dim_video v on f.video_id = v.video_id
join dim_category c on f.category_id = c.category_id
order by views desc
limit 10;

- Result

| video\_id   | title                                                           | title            | likes   |
| ----------- | --------------------------------------------------------------- | ---------------- | ------- |
| WtE011iVx1Q | SebastiÃ¡n Yatra - Por Perro ft. Luis Figueroa, Lary Over       | Music            | 396337  |
| 7C2z4GqqS5E | BTS (ë°©íƒ„ì†Œë…„ë‹¨) 'FAKE LOVE' Official MV                   | Music            | 3880074 |
| VTzD0jNdrmo | Rkm & Ken-Y âŒ Natti Natasha - Tonta \[Official Video\]        | Music            | 383030  |
| i0p1bmr0EmE | TWICE What is Love? M/V                                         | Music            | 1111592 |
| 6ZfuNTqbHE8 | Marvel Studios' Avengers: Infinity War Official Trailer         | Entertainment    | 1735931 |
| wfWkmURBNv8 | Ozuna x Romeo Santos - El Farsante Remix                        | Music            | 769384  |
| ePO5M5DE01I | Tiger Zinda Hai | Official Trailer | Salman Khan | Katrina Kaif | Film & Animation | 829362  |
| 2Vv-BfVoq4g | Ed Sheeran - Perfect (Official Music Video)                     | Music            | 1634130 |
| AhQcNVyndSM | Brytiago X Darell - Asesina ðŸ—¡                                | Music            | 299378  |
| 0XElmYomloA | Zion & Lennox - La Player (Bandolera) I Video Oficial           | Music            | 104931  |



### Top 10 the 10 most trending categories.

- Code

select c.category_id, c.title, sum(f.views) as total_views
from fact_youtube_trend f join dim_category c on f.category_id = c.category_id
group by c.category_id, c.title
order by sum(f.views) desc
limit 10;

- Result

| category\_id | title                | total\_views |
| ------------ | -------------------- | ------------ |
| 24           | Entertainment        | 1.08E+10     |
| 10           | Music                | 4.72E+09     |
| 22           | People & Blogs       | 3.74E+09     |
| 17           | Sports               | 2.94E+09     |
| 23           | Comedy               | 2.81E+09     |
| 25           | News & Politics      | 2.45E+09     |
| 1            | Film & Animation     | 1.69E+09     |
| 26           | Howto & Style        | 1.49E+09     |
| 20           | Gaming               | 1.01E+09     |
| 28           | Science & Technology | 7.92E+08     |
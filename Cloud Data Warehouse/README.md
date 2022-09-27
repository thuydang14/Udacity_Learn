# Project: Cloud Data Warehouse Sparkify

Project tree
.
├── cluster_implement.ipynb
├── create_tables.py
├── dwh.cfg
├── etl.py
├── __pycache__
│   └── sql_queries.cpython-36.pyc
├── README.md
└── sql_queries.py


Project Cloud Data Warehouse will help startup to move their processes
and data onto the cloud. Their data stored in S3, we will build ETL process to extract data from their S3 bucket
and stages into Redshift, and transform into set of dimensional model for their analytics team.

## Song data format
{
    "num_songs": 1,
    "artist_id": "ARJIE2Y1187B994AB7",
    "artist_latitude": null,
    "artist_longitude": null,
    "artist_location": "",
    "artist_name": "Line Renaud",
    "song_id": "SOUPIRU12A6D4FA1E1",
    "title": "Der Kleine Dompfaff",
    "duration": 152.92036,
    "year": 0
}


## Event data format
{
    "artist": "Explosions In The Sky",
    "auth": "Logged In",
    "firstName": "Layla",
    "gender": "F",
    "itemInSession": 87,
    "lastName": "Griffin",
    "length": 220.3424,
    "level": "paid",
    "location": "Lake Havasu City-Kingman, AZ",
    "method": "PUT",
    "page": "NextSong",
    "registration": 1541057188796,
    "sessionId": 984,
    "song": "So Long_ Lonesome",
    "status": 200,
    "ts": 1543449470796,
    "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/36.0.1985.125 Safari/537.36\"",
    "userId": "24"
}

## Schema
### songplays table: This table is fact table, will use to store song played by user
        songplay_id int identity(0,1) primary key,
        start_time timestamp not null sortkey distkey,
        user_id int not null,
        song_id char(50),
        artist_id char(50),
        session_id int,
        level char(10),
        location text,
        user_agent text
        
### users table: store data about user
        user_id int primary key sortkey,
        first_name varchar,
        last_name varchar,
        gender char(2),
        level char(10)
        
### songs table: Store data about songs
        song_id char(50) primary key sortkey,
        title varchar not null,
        artist_id char(50),
        year int,
        duration float not null
        
### artist table: Store data about artist.
        artist_id char(50) primary key sortkey,
        name varchar not null, 
        location varchar,
        latitude float,
        longtitude float
        
### time table: Store time data
        start_time timestamp primary key sortkey distkey,
        hour int,
        day int,
        week int,
        month int,
        year int,
        weekday text


## Build ETL Pipeline
1. Run *cluster_implement.ipynb* to create Redshift cluster, IAM role for access to S3
2. Run *create_table.py* to create staging table, table or drop table.
3. Run *etl.py* to execute ELT process



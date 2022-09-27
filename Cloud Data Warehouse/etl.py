import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function will load data from S3 to staging tables
    
    Arguments:
        cur: the cursor object
        conn: connection to postgresql using psycopg2.
    
    return: None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Description: This function will insert data from staging table to table.
    
    Arguments:
        cur: the cursor object
        conn: connection to postgresql using psycopg2
    
    return: None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
        Description: main function will read config from dwh.cfg file and connect to redshift cluster,
        then load data from s3 to staging table in cluster, and load data from staging table to table at last.
        Arguments: None
        Returns: None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
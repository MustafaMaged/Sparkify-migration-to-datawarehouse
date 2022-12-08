import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

## first we define the loading tables fn which copies the staging tables
def load_staging_tables(cur, conn):
    
    """  
    iterate on the copy table queries list saved
    in sql_queries.py and execute them  to load staging tables
    
    """
    
    for query in copy_table_queries:
        
        cur.execute(query)
        
        conn.commit()

## second we define the loading tables fn which inserts the star schema tables
def insert_tables(cur, conn):
    
    """  
    iterate on the insert table queries list saved
    in sql_queries.py and execute them  to load star schema tables
    
    """
        
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    #loading params
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    HOST=config.get('CLUSTER','HOST')
    DB_NAME=config.get('CLUSTER','DB_NAME')
    DB_USER=config.get('CLUSTER','DB_USER')
    DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
    DB_PORT=config.get('CLUSTER','DB_PORT')
    
    #connect to database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    
    
    conn.close()


if __name__ == "__main__":
    main()
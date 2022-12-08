import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# first we drop tables to ensure new tables are made on each run
def drop_tables(cur, conn):
    """  
    iterate on the drop table queries list saved
    in sql_queries.py and execute them 
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

# second we create all the tables, staging tables and star schema tables
def create_tables(cur, conn):
    """  
    iterate on the create table queries list saved
    in sql_queries.py and execute them 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # load params
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))
    HOST=config.get('CLUSTER','HOST')
    DB_NAME=config.get('CLUSTER','DB_NAME')
    DB_USER=config.get('CLUSTER','DB_USER')
    DB_PASSWORD=config.get('CLUSTER','DB_PASSWORD')
    DB_PORT=config.get('CLUSTER','DB_PORT')
    # connect to db
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST,DB_NAME,DB_USER,DB_PASSWORD,DB_PORT))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
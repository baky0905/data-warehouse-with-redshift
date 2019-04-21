import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Tables defined in the sql_queries.py loaded into redshift database.
    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Tables defined in the sql_queries.py are inserted from the staging tables 
    into the existing star schema tables in the redshift database.
    Parameters
    ----------
    cur
        Database cursor.
    conn
        Database connection.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that connects to the redshift database,
    loads staging tables and inserts existing tables from the staging tables.
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
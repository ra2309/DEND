import glob
import os
import psycopg2
import pandas as pd
from sql_queries import *




def create_database():
    # connect to default database
    conn = psycopg2.connect("host='denddb.postgres.database.azure.com' dbname='postgres' user='rmamnk@denddb' password='NPrhYs17U4' port='5432'")
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    
    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS oildb")
    cur.execute("CREATE DATABASE oildb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()    
    
    # connect to sparkify database
    conn = psycopg2.connect("host=denddb.postgres.database.azure.com dbname=oildb user=rmamnk@denddb password=NPrhYs17U4")
    cur = conn.cursor()
    
    return cur, conn


def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    cur, conn = create_database()
    
    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
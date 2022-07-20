import credentials
import io
from io import StringIO
import multiprocessing as mp
from multiprocessing import Pool
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from sqlalchemy import create_engine, func, distinct
import sqlalchemy.pool as pool
import tqdm

# Credential and connection settings
db_username = credentials.db_username
db_pwd = credentials.db_pwd
db_name = credentials.db_name
db_host = credentials.db_host
db_port = 5432

engine_string = f"postgresql://{db_username}:{db_pwd}@{db_host}:{db_port}/{db_name}"

db_engine = create_engine(engine_string)

def getconn():
    c = psycopg2.connect(user=db_username, host=db_host, dbname=db_name, password=db_pwd)
    return c

mypool = pool.QueuePool(getconn, max_overflow=10, pool_size=20)

def read_sql(query, db_engine):
    """ Read data from db and return a dataframe.

    Parameters:
        query (str): SQL query string
        db_engine (str): sqlalchemy create engine object
    Return:
        df = Pandas DataFrame
    """

    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
    conn = db_engine.raw_connection()
    cur = conn.cursor()
    store = io.StringIO()
    cur.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)
    cur.close()
    return df

def multi_read_sql(query):
    """ Read data from db and return a dataframe.

    Parameters:
        query (str): SQL query string
    Return:
        df = Pandas DataFrame
    """
    copy_sql = "COPY ({query}) TO STDOUT WITH CSV {head}".format(query=query, head="HEADER")
    conn = mypool.connect()
    cur = conn.cursor()
    store = io.StringIO()
    cur.copy_expert(copy_sql, store)
    store.seek(0)
    df = pd.read_csv(store)
    cur.close()
    conn.close()
    return df

def get_last_row_id(table):
    """Get last row_id from table

    Parameters:
        table (str): Name of table
    Return:
        last_tweet (int): Last row_id number.
    """

    # Select table to update
    search_table = table
    # SQL statement
    findCountSql = f"SELECT row_id FROM {table} ORDER BY row_id DESC LIMIT 1"
    # Execute sql query
    findCount = read_sql(findCountSql, db_engine)
    # Find the last tweet
    last_tweet = findCount['row_id'][0]
    print(f"{last_tweet} number of records")
    return last_tweet

def multi_with_progress(function_to_run, cores, last_tweet, batch_size):
    """Multiprocess a function on a database table.

    Parameters:
        function_to_run (function): Function to run
        cores (int): number of cores to use for multiprocessing
        last_tweet (int): length of table
        batch_size (int): Size of batch to fetch

    Returns:
        results (list): List of results.
    """
    batch_size = batch_size
    a_pool = mp.Pool(cores)
    inputs = range(0,last_tweet, batch_size)
    tot = len(inputs)
    results = []
    for result in tqdm.tqdm(a_pool.imap_unordered(function_to_run, inputs), total=len(inputs)):
        results.append(result)
    a_pool.close()
    a_pool.join()
    return results

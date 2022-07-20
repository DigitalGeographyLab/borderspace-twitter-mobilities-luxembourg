import csv
import geojson
import geopandas as gpd
import io
from io import StringIO
import json
import numpy as np
import operator
import os
import pandas as pd
import psycopg2
import psycopg2.extras as extras
from shapely.geometry import Point, LineString, Polygon
import tempfile


import sys
sys.path.insert(0,'../')
import db_connection as db_con
sys.path.insert(0,'../centroid')
from centroid import coordsToPolygon, getCentroid
sys.path.insert(0,'../information_extraction')
from info_extract import fromPlaceToCenterLat, fromPlaceToCenterLon, fromPlaceToType, getLat, getLon

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    Change to lat2, lon2 etc for luxemburg tables
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "UPDATE %s AS t SET lat = data.lat, lon = data.lon, spatial_level = data.spatial_level FROM (VALUES %%s) AS data (row_id,lat,lon,spatial_level) WHERE t.row_id = data.row_id" % (table)
    cursor = conn.cursor()
    try:
        print('Uploading')
        extras.execute_values(cursor, query, tuples, page_size=1000)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("execute_values() done")
    cursor.close()

def copy_from_stringio(conn, df, table):
    """
    Here we are going save the dataframe in memory 
    and use copy_from() to copy it to the table.
    Change to lat, lon etc if using other table
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=False)
    buffer.seek(0)
    cursor = db_con.conn.cursor()
    cursor.execute("CREATE TEMP TABLE tmp_table (id INT, row_id BIGINT, lat2 VARCHAR, lon2 VARCHAR, spatial_level VARCHAR(20));")
    try:
        cursor.copy_from(buffer, "tmp_table", sep=",")
        cursor.execute(f"UPDATE {table} SET lat2 = data.lat2, lon2 = data.lon2, spatial_level = data.spatial_level FROM tmp_table AS data WHERE {table}.row_id = data.row_id;")
        cursor.execute("DROP TABLE tmp_table")
        cursor.close()
        
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        cursor.close()
        return 1
    print("copy_from_stringio() done")
    #cursor.close()

# Select table to update
table = 'twitter_histories_luxemburg'
# Create connection
conn = db_con.db_engine.raw_connection()
# Initialize cursor
cur = conn.cursor()
# SQL statement
findCountSql = f"SELECT row_id FROM {table} ORDER BY row_id DESC LIMIT 1"
# Execute sql query
findCount = db_con.read_sql_inmem_uncompressed(findCountSql, db_con.db_engine)
# Find the last tweet
last_tweet = findCount['row_id'][0]
# Close connection
cur.close()
conn.close()
print(f"{last_tweet} number of records")

# Update parameters
batch_size = 1000000
start_number = 0
max_number = 1000000

percent = 0
percent_per_round = round((batch_size / last_tweet) * 100,2)
# Initialize connection
conn = db_con.psyco_con

for offset in range(0, last_tweet, batch_size):
    print('Starting')
    # SQL query
    sql_query = f"SELECT coordinates,lat,lon,place,row_id,id FROM {table} WHERE row_id <{max_number} AND row_id>={start_number} ORDER BY row_id ASC LIMIT {batch_size}"
    # Read results to dataframe
    df = db_con.read_sql_inmem_uncompressed(sql_query, db_con.db_engine)
    print('dataframe read')
    # Subset dataframe
    df = df[(df['coordinates'] != 'None') | (df['place'] != 'None')]
    # Extract data
    ##### For luxembourg tables #####
    df['lat2'] = df.apply(lambda row: fromPlaceToCenterLat(row) if row['coordinates'] == 'None' else row['lat'], axis=1)
    df['lon2'] = df.apply(lambda row: fromPlaceToCenterLon(row) if row['coordinates'] == 'None' else row['lon'], axis=1)
    
    df['spatial_level'] = df.apply(lambda row: fromPlaceToType(row) if row['coordinates'] == 'None' else 'GPS', axis=1)
    print('values extracted')
    
    # Subset to only columns to use for update
    df = df[['row_id','lat2','lon2','spatial_level']]
    

    # Push data to db
    execute_values(conn, df, table)

    # Delete temporary dataframe
    del df
    start_number += batch_size
    max_number += batch_size
    percent += percent_per_round
    print(f'{round(percent,2)}% done.')

cursor.execute(f"ALTER TABLE {table} ALTER COLUMN lat2 TYPE numeric USING NULLIF(lat2, '')::numeric, ALTER COLUMN lon2 TYPE numeric USING NULLIF(lon2, '')::numeric;")
conn.commit()
conn.close()

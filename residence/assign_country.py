import pandas as pd
import psycopg2
import psycopg2.extras as extras
import operator
import numpy as np
import geopandas as gpd
import json
from sqlalchemy import create_engine, func, distinct
import io
from io import StringIO
import tempfile
import os
import csv
from shapely.geometry import Point, LineString, Polygon
import geojson
import pickle
import sys
from pyproj import CRS
from datetime import datetime
import multiprocessing as mp
from multiprocessing import Pool
import db_connection as db_con

def execute_values(conn, df, table):
    """
    Using psycopg2.extras.execute_values() to insert the dataframe
    Change to lat2, lon2 etc if using luxembourg tables
    """
    # Create a list of tupples from the dataframe values
    tuples = [tuple(x) for x in df.to_numpy()]
    # Comma-separated dataframe columns
    cols = ','.join(list(df.columns))
    # SQL quert to execute
    query  = "UPDATE %s AS t SET post_country = data.post_country FROM (VALUES %%s) AS data (row_id,post_country) WHERE t.row_id = data.row_id" % (table)
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

def getCountry(row):
    # Create point
    point = Point(row['lon2'], row['lat2'])
    # Loop over the Greater Luxemburg region
    for idx, region in greater_lux_region.iterrows():
        if point.within(region['geometry'])== True:
            return 'GRL'
        else:
            pass
    # Loop over countries
    for idx, country in world.iterrows():
        if point.within(country['geometry']) == True:
            return country['Country']
        else:
            pass
    # Find nearest country
    polygon_index = world.distance(point).sort_values().index[0]
    
    return world['Country'].loc[polygon_index]



def getClosestCountry(row):
    point = Point(row['lon2'], row['lat2'])
    polygon_index = world.distance(point).sort_values().index[0]
    return world['Country'].loc[polygon_index]

def funcToRun(df):
    df['Country'] = df.apply(lambda row: getClosestCountry(row), axis=1)
    return df

def parallelize_dataframe(df, func, n_cores=4):
    df_split = np.array_split(df, n_cores)
    pool = Pool(n_cores)
    df = pd.concat(pool.map(func, df_split))
    pool.close()
    pool.join()
    return df
def overlayOutside(gdf):
    gdf = gpd.overlay(gdf,inland, how='difference')
    return gdf

# Select table to update
table = 'lux_histories_geo'
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

world = gpd.read_file(r"Global_regions_GRL.gpkg")

greater_lux_region = world.loc[world['GreaterLux'] == 1]

# Update parameters
batch_size = 250000
start_number = 0
max_number = 250000
table = "lux_histories_geo"
conn = db_con.psyco_con



def convertLuxCountry(row):
    if row['GreaterLux'] == 1.0:
        return 'GRL'
    else:
        return row['Country']

for offset in range(0, last_tweet, batch_size):
    starttime = datetime.now()
    query = f'SELECT * FROM {table} WHERE row_id <{max_number} AND row_id>={start_number} ORDER BY row_id ASC LIMIT {batch_size}'
    data = db_con.read_sql_inmem_uncompressed(query, db_con.db_engine)
    print(f'Data read, max_number: {max_number}')
    gdf = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data['lon2'], data['lat2']))
    print('GeoDataFrame created.')
    gdf.crs = "EPSG:4326"
    gdf = gdf.to_crs(world.crs)
    del data
    print('DataFrame 1 deleted.')
    inland = gpd.sjoin(gdf,world, how='inner', op='within')
    print('Spatial join done.')
    inland = inland[['created_at', 'id', 'lang', 'user_id', 'user_loc', 'lat2', 'lon2', 'spatial_level', 'row_id', 'geometry','Country', 'GreaterLux']]
    outside = parallelize_dataframe(gdf, overlayOutside, 5)
    print('Difference calculation done.')
    outside = outside.sort_values(by=['row_id'])
    outside = parallelize_dataframe(outside, funcToRun, 6)
    print('Closest countries found.')
    del gdf
    print('gdf deleted.')
    inland = pd.DataFrame(inland)
    outside = pd.DataFrame(outside)
    
    result = pd.concat([inland,outside])
    result = result.sort_values(by=['row_id'])
    result['post_country'] = result.apply(lambda row: convertLuxCountry(row), axis=1)
    result = result[['row_id','post_country']]

    execute_values(conn, result, table)
    start_number += batch_size
    max_number += batch_size
    endtime = datetime.now()
    del inland
    del outside
    del result
    print(f'Batch took: {endtime-starttime}')


conn.commit()
conn.close()
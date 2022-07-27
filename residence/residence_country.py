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
import sqlalchemy.pool as pool
import psycopg2
import multiprocessing as mp
from multiprocessing import Pool
import db_connection as db_con
import random
print('Start')
starttime = datetime.now()


def crosstabulate_users_posts_per_region(df, region_column):
    """Crosstabulate users posts per region"""

    post_count_matrix = pd.crosstab(df.userid, df[region_column])
    return post_count_matrix


def get_maxtimedelta_region(df, region_column):
    """ Calculate max time delta in each region for each user"""

    # Crosstabulate min and max timestamp for each user in each country (if they'be been there)
    crossTmin = pd.crosstab(df.userid, df[region_column], values=df.time_local, aggfunc=pd.Series.min)
    crossTmax = pd.crosstab(df.userid, df[region_column], values=df.time_local, aggfunc=pd.Series.max)

    # Calculate time difference for each user in each country. Returns a matrix of time deltas
    #NOTE: TAKING THE ABSOLUTE VALUE OF TIME DELTA
    timedelta_matrix = abs(crossTmin - crossTmax)

    # Get max time-difference and associated region
    timecol = "MAXTimeDeltas" 

    timedelta_matrix[timecol] = timedelta_matrix.max(axis=1)
    #timedelta_matrix[regioncol] = timedelta_matrix.idxmax(axis=1)

    # Use another function to detect if user has posted equally as often from many countries, add related columns
    timedelta_matrix = list_maxtime_countries(df, timedelta_matrix, region_column, timecol="TimeDelta")

    # convert max time diff to format: days
    timedelta_matrix[timecol] = timedelta_matrix[timecol].apply(lambda x: x.days)

    return timedelta_matrix


def get_maxtime_region(df, region_column, timecol, truncate=True):
    """Cross-tabulate number of unique time units (days, months, weeks) in each country, and get country or 
    list of countries where this value is highest into a new column"""
    
    #print("Crosstabulating number of unique %ss for each users within each region..." % timecol)
    time_matrix = pd.crosstab(df.userid, df[region_column], values=df[timecol], aggfunc=pd.Series.nunique)

    # Generate column names
    most_frequent_region = "%s_withMax%s" % (region_column, timecol)
    max_time_units = "MAX%ss" % timecol

    # Get max time-difference and associated region
    time_matrix[max_time_units] = time_matrix.max(axis=1)

    # NB! this would be wrong if multiple countries have the same number of time units!
    #time_matrix[most_frequent_region] = TimeMatrix_copy.idxmax(axis=1)

    if truncate:
        # take only info of the country with maximum posting frequency
        time_matrix = time_matrix[[most_frequent_region, max_time_units]]

    #Use another function to detect if user has posted equally as often from many countries, add related columns
    time_matrix = list_maxtime_countries(df, time_matrix, region_column, timecol)

    return time_matrix


def list_maxtime_countries(df, time_matrix, region_column, timecol):
    """Count how many countries have the max number of time units and 
    List region codes of those regions which have equally many time units"""

    max_t_column = "MAX%ss" % (timecol)
    count_column = "N_of_%s_withMax%s" % (region_column, timecol)

    # Check how many potential values there are (how many columns in time matrix with country codes)
    datarange = df[region_column].nunique()

    #Count how many countries have the max number of time units
    for i, row, in time_matrix.iterrows():
        time_matrix.loc[i, count_column] = (row[:datarange] == row[max_t_column]).sum()

    # List Top Regions (check those instances where the number of time units equals to the max time unit) row by row
    # (x.index is a series of the true values in each row!)
    # In other words; check the value range where country info is located using demo_data range,
    # and check which items are equal to the max time units
    time_matrix["HomeLocList"] = time_matrix.apply(lambda x: x.index[:datarange][x[:datarange] == x[max_t_column]].tolist(), axis=1)

    return time_matrix


def get_regional_post_counts(user, countrylist, postcountDF):
    """ Get user-specific count of posts per region"""
    
    #Check how many countries this user has as candidates
    n_home_candidates = len(countrylist)

    #Dictionary for collecting candidates
    candidates = {}

    # Go trough countries, and find number of users for each country
    for listposition in range(n_home_candidates):

        # Populate the dictionary with region code and associated number of posts from this user
        candidates[countrylist[listposition]] = postcountDF.loc[user, countrylist[listposition]]
        
    return candidates


def apply_maxposts_to_candidate_countries(candidates):
    """In case the result contains two countries, this method check which of those countries have
    most posts based on a dictionary that contains the country code and post count. If there are several options
    after checking the number of posts, then the origin country is selected randomly among these.
    
    :param candidates: dictionary of users posts per country
    :return: Country with most posts
    """
    #Detect all items which have the maximum value
    home_loc = [key for key, value in candidates.items() if value == max(candidates.values())]

    if len(home_loc) == 1:
        return str(home_loc[0])
    else:
        # RANDOM COUNTRY OUT OF THE CANDIDATES (these countries have equal number of maxtime, and maxposts)
        return random.choice(home_loc)
    
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

# --------------------------
# SOCIAL MEDIA DATA
# --------------------------

# Social media mobility history for twitter users
# each point is assigned to the nearest region if found not on land. Also duplicates have been removed
# Select table to update
table = 'lux_histories_geo'
# Create connection
conn = db_con.db_engine.raw_connection()
# Initialize cursor
cur = conn.cursor()
# SQL statement
query = f"SELECT * FROM {table}"
# Execute sql query
data = db_con.read_sql_inmem_uncompressed(query, db_con.db_engine)
# Find the last tweet
# Close connection
cur.close()
conn.close()
print('Data read')
# read demo_data with geopandas
#some = gpd.read_file(fp)
some = gpd.GeoDataFrame(data, geometry=gpd.points_from_xy(data.lon2,data.lat2))
print('GeoDataFrame created.')
some = some.rename(columns={'user_id': 'userid', 'post_country':'FIPS', 'created_at':'time_local'})
# Print layer info
print("Number of posts:", len(some))
print("Number of users:", some.userid.nunique())



# Crosstabulate number of posts per country (needed if unambiquous result from temporal method)
print('Crosstabulating:')
users_posts_per_country = crosstabulate_users_posts_per_region(some, "FIPS")
# -----------------------------------------------
# MAX TIME DIFFERENCE WITHIN REGION
# -----------------------------------------------


# convert to datetime
some["time_local"] = pd.to_datetime(some["time_local"])

# Separate months, weeks and days into own columns for cross-tabulation:
some["month"] = some.time_local.dt.to_period('M') #some["time_local"].apply(lambda x: x.month)
some["week"] = some.time_local.dt.to_period('W')
some["day"] = some.time_local.dt.to_period('D')
# -----------------------------
# MAX MONTHS; WEEKS and DAYS
# -----------------------------
def weekAndDay(some):
	time = 'week'
	week_results = get_maxtime_region(some, 'FIPS', timecol=time, truncate=False)
	# Deal with unambiguous results:
	week_results["userid"] = week_results.index
	week_results["WeekHomeLocDict"] = week_results.apply(lambda x: get_regional_post_counts(x["userid"], x["HomeLocList"], users_posts_per_country), axis=1)
	week_results['basic_maxweek'] = week_results.apply(lambda x: apply_maxposts_to_candidate_countries(x["WeekHomeLocDict"]), axis=1)
	week_results = week_results[['MAX%ss' % time, 'N_of_FIPS_withMax%s' % time, 'HomeLocList', 'WeekHomeLocDict', 'basic_maxweek']]
	week_results = week_results.rename(columns = {'MAX%ss' % time: 'FIPS_%scount' % time})
	week_results = week_results.reset_index()
	print('Week done')
	time = 'day'
	day_results = get_maxtime_region(some, 'FIPS', timecol=time, truncate=False)
	# Deal with unambiguous results:
	day_results["userid"] = day_results.index
	day_results["DayHomeLocDict"] = day_results.apply(lambda x: get_regional_post_counts(x["userid"], x["HomeLocList"], users_posts_per_country), axis=1)
	day_results['basic_maxdays'] = day_results.apply(lambda x: apply_maxposts_to_candidate_countries(x["DayHomeLocDict"]), axis=1)
	day_results = day_results[['MAX%ss' % time, 'N_of_FIPS_withMax%s' % time, 'HomeLocList', 'DayHomeLocDict', 'basic_maxdays']]
	day_results = day_results.rename(columns = {'MAX%ss' % time: 'FIPS_%scount' % time})
	day_results = day_results.reset_index()
	print('Day done')
	results = pd.merge(week_results, day_results, on='userid')
	results.reset_index()
	results.columns.name = None
	return results
print('Ready to multiprocess.')
results = parallelize_dataframe(some, weekAndDay, 28)
def assignResidence(row):
    if row['N_of_FIPS_withMaxweek'] == 1.0:
        return row['HomeLocList_x'][0]
    elif row['N_of_FIPS_withMaxday'] == 1:
        return row['HomeLocList_y'][0]
    else:
        return row['basic_maxdays'][0]

results['residence_country'] = results.apply(lambda row: assignResidence(row), axis=1)
residence = results[['userid','residence_country']]

print(residence)

total_time = datetime.now()-starttime
print(f"Script took: {total_time}")
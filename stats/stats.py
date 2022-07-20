import sys
sys.path.insert(0,'../')
import db_connection as db_con
import pandas as pd
import psycopg2
import psycopg2.extras as extras
import pickle

# Set up database connection
psyco_con = db_con.psyco_con

# Define table name
table = 'twitter_histories_luxemburg_combined2'

def getDistinctValues(column_name ,table_name, con):
    # Start cursor
    cur = con.cursor()
    # SQL string
    #columnFind = f"SELECT DISTINCT {column_name} FROM {table_name} ORDER BY {column_name};"
    columnFind = f"SELECT {column_name} FROM {table_name} GROUP BY {column_name};"
    print(columnFind)
    # Run SQL
    cur.execute(columnFind)
    # Get all results into a list
    values = [r[0] for r in cur.fetchall()]
    # Close cursor
    cur.close()
    return values


# Get list of different spatial levels
spatialLevelsList = getDistinctValues('spatial_level',table, psyco_con)
with open('spatialLevelsList.pkl', 'wb') as f:
    pickle.dump(spatialLevelsList,f)

with open('spatialLevelsList.pkl', 'rb') as f:
    spatialLevelsList = pickle.load(f)
# Get list of user id's
userIdList = getDistinctValues('user_id',table, psyco_con)
with open('userIdList.pkl', 'wb') as f:
    pickle.dump(userIdList,f)
with open('userIdList.pkl', 'rb') as f:
    userIdList = pickle.load(f)


# Create cursor
cur = psyco_con.cursor()
# Create dict
dct = {}
# Loop over each spatial level
for level in spatialLevelsList:
    print(level)
    # SQL query
    sql_query = f"SELECT user_id, COUNT(spatial_level) FROM {table} WHERE spatial_level = '{level}' GROUP BY user_id;"
    # Execute query
    cur.execute(sql_query)
    results = cur.fetchall()
    users = [a_tuple[0] for a_tuple in results]
    zeroUsers = [item for item in userIdList if item not in users]
    # Fetch query and store in dict
    dct['count_%s' % level] = [{'user':r[0],level:r[1]} for r in results]
    for user in zeroUsers:
        dct['count_%s' % level].append({'user':user, level:0})

# Close cursor
cur.close()

with open('dicts.pkl', 'wb') as f:
    pickle.dump(dct,f)
with open('dicts.pkl', 'rb') as f:
    dct = pickle.load(f)



# Printing length of dicts (should be the same as lenght of userIdList)
print(f"Number of users: {len(userIdList)}")
print(f"Length of None: {len(dct['count_None'])}")
print(f"Length of neighborhood: {len(dct['count_neighborhood'])}")
print(f"Length of city: {len(dct['count_city'])}")
print(f"Length of admin: {len(dct['count_admin'])}")
print(f"Length of country: {len(dct['count_country'])}")
print(f"Length of GPS: {len(dct['count_GPS'])}")
print(f"Length of poi: {len(dct['count_poi'])}")
print('---------------')


# Adding dict with total number of tweets per user
cur = psyco_con.cursor()
sql_query = f"SELECT user_id, COUNT(user_id) FROM {table} GROUP BY user_id;"
cur.execute(sql_query)
dct['total'] = [{'user':r[0],'total':r[1]} for r in cur.fetchall()]
cur.close()
psyco_con.close()
print(f"Length of total: {len(dct['total'])}")

# Use pre-created total dictionaries
#with open('totalDicts.pkl', 'rb') as f:
#    dct = pickle.load(f)

# Creating empty columns list
columns = []
for dicts in dct:
    columns.append(dicts)
# Adding a user column
columns.append('user')
# Creating dataframe from total number of tweets per user
statsDf = pd.DataFrame(dct['total'])
# Setting index to user
statsDf = statsDf.set_index('user')
# Looping over dicts
for dicts in dct:
    # Create dataframe
    newDf = pd.DataFrame(dct[dicts])
    # Joining dataframes
    statsDf = statsDf.join(newDf.set_index('user'), on='user', lsuffix='_og', rsuffix='_new')

# Drop repeated column
statsDf = statsDf.drop(columns=['total_new'])
# Create cummulative column
print(statsDf.columns)
statsDf['cummu'] = statsDf['neighborhood']+statsDf['city']+statsDf['admin']+statsDf['country']+statsDf['GPS']+statsDf['poi']
print(statsDf.head())
# Save dataframe to file
statsDf.to_csv(f'{table}_spatial_level_stats.csv')

# Display info
print(statsDf.describe())
print(f"Total GPS points: {statsDf['GPS'].sum()}")
print(f"Total neighborhood points: {statsDf['neighborhood'].sum()}")
print(f"Total poi points: {statsDf['poi'].sum()}")
print(f"Total city points: {statsDf['city'].sum()}")
print(f"Total country points: {statsDf['country'].sum()}")
print(f"Total admin points: {statsDf['admin'].sum()}")
print(f"Total none points: {statsDf[''].sum()}")
print(f"Total Cummulative points: {statsDf['cummu'].sum()}")
print(f"Total number of tweets: {statsDf['total_og'].sum()}")
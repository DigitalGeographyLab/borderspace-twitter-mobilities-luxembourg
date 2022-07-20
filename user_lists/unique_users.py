#!/usr/bin/env python
# coding: utf-8

# Imports
import pandas as pd
import operator
import numpy as np
import geopandas as gpd
import json
import tempfile
import os
import csv
import sys
from datetime import datetime
sys.path.insert(0,'../')
import db_connection as db_con

starttime = datetime.now()

# Get length of table
table = 'twitter_histories_luxemburg_combined2'
last_tweet = db_con.get_last_row_id(table)


def get_user_ids_batch(start_number):
    max_number = start_number+batch_size
    query = f'SELECT user_id, row_id, created_at FROM {table} WHERE row_id <{max_number} AND row_id>={start_number} ORDER BY row_id ASC LIMIT {batch_size}'
    data = db_con.multi_read_sql(query)
    user_list = list(data['user_id'].unique())
    del data
    return user_list

# First table_list
start_time = '2012-07-17 22:57:06'
end_time = '2016-01-01 4:59:45'
batch_size = 250000
result = db_con.multi_with_progress(get_user_ids_batch, 31, last_tweet, batch_size)
all_users = [item for sublist in result for item in sublist]
del result
unique_users = list(dict.fromkeys(all_users))
del all_users

with open('lux_user_list_1.csv', 'w') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
    write.writerows([unique_users])

# Second table_list
table = "additional_data"
last_tweet = db_con.get_last_row_id(table)
result = db_con.multi_with_progress(get_user_ids_batch, 31, last_tweet, batch_size)
all_users = [item for sublist in result for item in sublist]
del result
unique_users2 = list(dict.fromkeys(all_users))
del all_users

with open('lux_user_list_2.csv', 'w') as f:
      
    # using csv.writer method from CSV package
    write = csv.writer(f)
    write.writerows([unique_users2])


print(f'Script took: {datetime.now()-starttime}')
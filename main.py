#!/usr/bin/env python
# coding: utf-8

# In[1]:


from collections import namedtuple
from joblib import Parallel, delayed
import requests

import pandas as pd
import arrow


# In[18]:


import logging


# In[2]:


API = '86b10fc39b08af63d279caba40862fb5'


# In[3]:


# df = pd.read_clipboard(names=['city','lat1', 'lat2', 'lon1', 'lon2', 'time'])
# df.to_csv('geo_latlon.csv', index=False)


# In[4]:


geoc = pd.read_csv('geo_latlon.csv', index_col=0)


# In[5]:


def get_temp(rec, dts):
    params = {
    'units': 'metric',
    'exclude': 'current,minutely,hourly,alerts',
    'appid': API,
    'lat': rec.lat1,
    'lon': rec.lon1,
    'dt':dts
    }
    URL = 'https://api.openweathermap.org/data/2.5/onecall/timemachine'
    try:
        resp = requests.get(URL, params=params).json()
        temp = resp['current']['temp']
    except e:
        logging.error(resp['message'])
    return {'location': rec.Index, 'temp': temp, 'dts':dts}


# In[6]:


# last 5 days timestamp, make sure of time indempotency
last_5d_ts = [arrow.utcnow().shift(days=-e).timestamp for e in range(5)]


# In[7]:


# Use all available threads to pull the data
data = []
for ts in last_5d_ts:
    data_temps = Parallel(n_jobs=-1)(delayed(get_temp)(loc,ts) for loc in geoc.itertuples())
    data.append(data_temps)


# In[8]:


df = pd.concat([pd.DataFrame(d) for d in data])


# In[9]:


# Change date format back to datetime
df['date'] = pd.to_datetime(df['dts'],unit='s', infer_datetime_format=True)


# In[10]:


max_temp = df.groupby(['location', df.date.dt.month]).temp.max().reset_index()


# In[11]:


max_temp.head()


# In[12]:


df.sort_values(['date', 'temp'], inplace=True)


# In[13]:


agg2 = (df
     .groupby('date')
     .agg({'temp': ['first', 'last'],
           'location': ['first', 'last']}))


# In[14]:


agg2.columns = ['min_temp', 'max_temp', 'loc_min_temp', 'loc_max_temp']


# In[15]:


agg2.head()


# In[16]:


max_temp.to_csv(f"data/data1_{arrow.utcnow().format('YYYY-MM-DD HH:MM:SS')}.csv")
agg2.to_csv(f"data/data2_{arrow.utcnow().format('YYYY-MM-DD HH:MM:SS')}.csv")


# In[ ]:


# in case we want to write to sql,
# import sqlalchemy as sa
# con = sa.create_engine('postgresql://user:pass@host/db_name')
# max_temp.to_sql('data1', con=con, index=False, if_exists='append')
# agg2.to_sql('data2', con=con, index=False, if_exists='append')


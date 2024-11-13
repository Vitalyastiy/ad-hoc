#%%
# cron на 23:20 ежедневно
import pandas as pd
import numpy as np
from telebot import types
from datetime import datetime, timedelta
import time
import os   
import io
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from urllib.parse import quote
from urllib.parse import quote_plus
import json

#PostgreSQL
HOST = 'c-c9q2d1as6cr7qicvb1ge.rw.mdb.yandexcloud.net' #мастер 
PORT = 6432
DATABASE = 'test'
USER = 'test_analytics_bi_user'
#PASSWORD = 
with open('/home/flerinvs/lab/pass.json') as f: 
             PASSWORD = json.load(f)["PASSWORD"]

engine = create_engine(f'postgresql://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}')


host = 'c-c9q3l6ni6fb59cfl329s.rw.mdb.yandexcloud.net' #мастер 
port = 6432
database = 'superset-prod'
#password_ss = 
username = 'superset-prod'
with open('/home/flerinvs/lab/pass.json') as f: 
             password_ss = json.load(f)["password_ss"]
             
engine_2 = create_engine(f'postgresql://{username}:{password_ss}@{host}:{port}/{database}', connect_args={"connect_timeout": 10})





# , CASE WHEN t2.email like '%vtb%' THEN 1 ELSE 0 END AS vtb
sql = '''

with t as (
SELECT t.id
    , t.action
    , t.user_id 
    , date_trunc('day', t.dttm) as dttm
    , t.dashboard_id 
--    , t.duration_ms
    , t1.dashboard_title
    , t1.published
    , t2.username
    , t2.active
    , t2.email
    , CASE WHEN t2.username IN ('flerinvs', 'akimovma', 'vasyaginmr', 'ginsburgea', 
    'kulalaevama', 'balitskayaav', 'mikhaelyanta', 'klekovkinve', 'yakovlevaev','furletovaav',
      'matveevia', 'fatinayun', 'admin', 'nikitindd', 'manaenkovaoa', 'marchenkoal', 'khabarovam',
        'davydovae', 'smirnovdm', 'lahvichds', 'salishevae', 'nagrimanovaat', 'yumatovad',
          'nikishinpa', 'akimovaav', 'godunovdp', 'gorbenkoid', 'sutuginaf') THEN 1 ELSE 0 END AS analytics

 --   ,DENSE_RANK() OVER (PARTITION BY t2.username ORDER BY t.dttm) AS rn,
  --  DENSE_RANK() OVER (PARTITION BY t1.dashboard_title ORDER BY t.dttm) AS rn_db
          
FROM public.logs t 
LEFT JOIN public.dashboards t1 ON t.dashboard_id = t1.id
LEFT JOIN public.ab_user t2 ON t.user_id = t2.id
WHERE t.action IN ('DashboardRestApi.get') 
AND t.dashboard_id != 11 
AND t.dashboard_id IS NOT NULL
ORDER BY t.dttm
)


select count(id) id,
action, user_id, dttm, dashboard_id, 
dashboard_title, published, username, 
active, email, analytics
,   DENSE_RANK() OVER (PARTITION BY dashboard_id ORDER BY dttm) AS rn_db
,   DENSE_RANK() OVER (PARTITION BY username ORDER BY dttm) AS rn

from t
group by action, user_id, dttm, dashboard_id, 
dashboard_title, published, username, 
active, email, analytics;
'''
df = pd.read_sql(sql, engine_2)
#, 'ChartDataRestApi.data', 'root', 'log', 'dashboard', 'explore_json', 'ExploreRestApi.get'

not_done = True
cnt = 1
while not_done and cnt < 5:
    cnt += 1
    try:
        # Your existing code to create the table
        df.to_sql(
            "LOGS_SS",
            engine,
            index=False,
            chunksize=10000,
            schema="USER_FLERINVS",
            if_exists='replace'
        )
        not_done = False

         
    except OperationalError as e:
        print('No connection to the server')
        

# %%
# SELECT t.id
#     , t.action
#     , t.user_id 
#     , date_trunc('day', t.dttm) as dttm
#     , t.dashboard_id 
#     , t.duration_ms
#     , t1.dashboard_title
#     , t1.published
#     , t2.username
#     , t2.active
#     , t2.email
#     , CASE WHEN t2.username IN ('flerinvs', 'akimovma', 'vasyaginmr', 'ginsburgea', 
#     'kulalaevama', 'balitskayaav', 'mikhaelyanta', 'klekovkinve', 'yakovlevaev','furletovaav',
#       'matveevia', 'fatinayun', 'admin', 'nikitindd', 'manaenkovaoa', 'marchenkoal', 'khabarovam',
#         'davydovae', 'smirnovdm', 'lahvichds', 'salishevae', 'nagrimanovaat', 'yumatovad',
#           'nikishinpa', 'akimovaav', 'godunovdp', 'gorbenkoid', 'sutuginaf') THEN 1 ELSE 0 END AS analytics,
#     DENSE_RANK() OVER (PARTITION BY t2.username ORDER BY t.dttm) AS rn,
#     DENSE_RANK() OVER (PARTITION BY t1.dashboard_title ORDER BY t.dttm) AS rn_db
# FROM public.logs t 
# LEFT JOIN public.dashboards t1 ON t.dashboard_id = t1.id
# LEFT JOIN public.ab_user t2 ON t.user_id = t2.id
# WHERE t.action IN ('DashboardRestApi.get') 
# AND t.dashboard_id != 11 
# AND t.dashboard_id IS NOT NULL
# ORDER BY t.dttm;
#%%
import pandas as pd 
import numpy as np

# %%
df = pd.read_csv('results.csv')
df
# %%
df.columns
# %%
df.info()
# %%
df1 = df[['report_month', 'region',	'nps',	'id', 'main_operator',  'sat_reason_1', 
    'sat_reason_2', 'sat_reason_3',
    'sat_reason_4',	'sat_reason_5',	'sat_reason_6',	'sat_reason_7',	'sat_reason_8',
    'sat_reason_9',	'sat_reason_10',	'sat_reason_14',	'sat_reason_11',	'sat_reason_12',
    'sat_reason_15',	'sat_reason_16',	'diss_reason_1',
    'diss_reason_2', 'diss_reason_3',	'diss_reason_4', 'diss_reason_5', 'diss_reason_6',
    'diss_reason_7', 'diss_reason_8',	'diss_reason_9',	'diss_reason_10', 
    'diss_reason_11', 'diss_reason_12', 'diss_reason_14',	'diss_reason_13',
    'diss_reason_15',	'diss_reason_16'
  ]]

#%%
dfc = df1.copy()

# %%
dfc['report_month'] =   dfc['report_month'].astype(str) + ' 01'
dfc['report_month'] = pd.to_datetime(dfc['report_month']) 
# %%
dfc_r = dfc.copy()
dfc_r
#= dfc.query('nps_type_ != "other"')#.to_excel('r.xlsx')
#%%
dfc_r = dfc_r.query('report_month >= "2022-07-01" and main_operator in \
           ("megafon", "beeline", "tele2", "mts" ) and nps > 0' )
#%%
dfc_r['sat_reason_13'] = np.nan
dfc_r.columns
# %%
df_unpivot_s = pd.melt(dfc_r
            , id_vars=['report_month', 'region', 'main_operator', 'nps', 'id']
            , value_vars=[  'sat_reason_1', 'sat_reason_2'
            , 'sat_reason_3', 'sat_reason_4', 'sat_reason_5', 'sat_reason_6'
            , 'sat_reason_7', 'sat_reason_8', 'sat_reason_9'
            , 'sat_reason_10', 'sat_reason_11', 'sat_reason_12','sat_reason_13'
            , 'sat_reason_14', 'sat_reason_15', 'sat_reason_16'
            ],  var_name='sat', value_name='subs_sat')
df_unpivot_s

#%%
df_unpivot_d= pd.melt(dfc_r
            , id_vars=['report_month', 'region', 'main_operator', 'nps', 'id']
            , value_vars=[  'diss_reason_1', 'diss_reason_2', 'diss_reason_3',
            'diss_reason_4', 'diss_reason_5', 'diss_reason_6',
            'diss_reason_7', 'diss_reason_8', 'diss_reason_9',
            'diss_reason_10', 'diss_reason_11', 'diss_reason_12',
            'diss_reason_14', 'diss_reason_13', 'diss_reason_15',
            'diss_reason_16'
            ],  var_name='diss', value_name='subs_dis')
df_unpivot_d
#%%

df_mer = pd.merge(df_unpivot_s, df_unpivot_d[['diss',	'subs_dis']], left_index= True, right_index=True, how='left')
df_mer
#%%
def flfunc(cnt):
  if cnt ['sat'] == 'sat_reason_1':
    return "Качество связи"
  elif cnt ['sat'] == 'sat_reason_2':
    return "Зона приёма"
  elif cnt ['sat'] == 'sat_reason_3':
    return "Тарифы"  
  elif cnt ['sat'] == 'sat_reason_4':
    return "Роуминг в поездках за рубеж" 
  elif cnt ['sat'] == 'sat_reason_5':
    return "Роуминг в поездках по России"   
  elif cnt ['sat'] == 'sat_reason_6':
    return "Платежи" 
  elif cnt ['sat'] == 'sat_reason_7':
    return "НЕ/Честность оператора" 
  elif cnt ['sat'] == 'sat_reason_8':
    return "Мобильный Интернет" 
  elif cnt ['sat'] == 'sat_reason_9':
    return "Абонентская служба" 
  elif cnt ['sat'] == 'sat_reason_10':
    return "СМС/ММС" 
  elif cnt ['sat'] == 'sat_reason_11':
    return "Мобильное приложение" 
  elif cnt ['sat'] == 'sat_reason_12':
    return "НЕ/Навязчивые звонки от оператора" 
  elif cnt ['sat'] == 'sat_reason_13':
    return "Навязчивые звонки от представителей банков / интернет-магазинов / других отраслей " 
  elif cnt ['sat'] == 'sat_reason_14':
    return "Дополнительные услуги " 
  elif cnt ['sat'] == 'sat_reason_15':
    return "Другое" 
  elif cnt ['sat'] == 'sat_reason_16':
    return "Затрудняюсь ответить " 
#%%
#df1["nps_type"] = df1.apply(nps_t, axis=1)
df_mer["reasons"] = df_mer.apply(flfunc, axis=1)
df_mer 
#%%
def func_avg(nps):
  if nps ['subs_sat'] == 1 or nps ['subs_dis']==1:
    return nps ["nps"]
  else:
    return "Пусто"  
#%%
df_mer["nps_purified"] = df_mer.apply(func_avg, axis = 1)
df_mer
#%%
df_mer.to_csv('prom_fin.csv')

#%%
'''df_mer1 =df_mer.groupby(['report_month', 'region',	'main_operator', 'nps', 
                'sat', 'diss', 'reasons', 'nps_purified' ]).agg\
  ({
    'subs_sat': ['sum'],
    'subs_dis': ['sum']   
  }
  ).reset_index()

df_mer1.columns = ['_'.join(col).strip() for col in df_mer1.columns.values]
df_mer1
#.query('nps_purified not in ("Пусто")').to_excel('prom_fin3.xlsx')
df_mer1['sum_sub'] = df_mer1['subs_sat_sum'] + df_mer1['subs_dis_sum']
df_mer1'''
#%%


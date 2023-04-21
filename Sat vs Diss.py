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
    'sat_reason_15',	'sat_reason_16',	'sat_reason_17',	'diss_reason_1',
    'diss_reason_2', 'diss_reason_3',	'diss_reason_4', 'diss_reason_5', 'diss_reason_6',
    'diss_reason_7', 'diss_reason_8',	'diss_reason_9',	'diss_reason_10', 
    'diss_reason_11', 'diss_reason_12', 'diss_reason_14',	'diss_reason_13',
    'diss_reason_15',	'diss_reason_16',	'diss_reason_17', 'sat_reason_mono',
    'diss_reason_18', 'diss_reason_new_17', 'diss_reason_19', 'diss_reason_20'
  ]]

#%%
def nps_t(count):
  if count ['nps'] > 0 and count ['nps'] < 7:
    return -1
  elif   9 > count ['nps'] > 6:
    return 0
  elif   count ['nps'] > 8:
    return 1
  else:
    return 'other'
#%%
df1["nps_type"] = df1.apply(nps_t, axis=1)
df1
#%%  
#dfc_r= df1.query('nps_type != "other"')#.to_excel('r.xlsx')
df1
# %%
#dfc_r.to_excel('sssss.xlsx')

#%%
df_cnt = df1.groupby(['report_month', 'region',	'main_operator', 'nps_type']).agg\
  ({
    'id':['count'],
    'sat_reason_1': ['sum'],
    'sat_reason_2': ['sum'], 
    'sat_reason_3': ['sum'],
    'sat_reason_4': ['sum'],	
    'sat_reason_5': ['sum'],	
    'sat_reason_6': ['sum'],	
    'sat_reason_7': ['sum'],
    'sat_reason_8': ['sum'],
    'sat_reason_9': ['sum'],
    'sat_reason_10': ['sum'],
    'sat_reason_11': ['sum'],
    'sat_reason_12': ['sum'],
    'sat_reason_14': ['sum'],
    'sat_reason_15': ['sum'],
    'sat_reason_16': ['sum'],
    'diss_reason_1': ['sum'],
    'diss_reason_2': ['sum'],
    'diss_reason_3': ['sum'],
    'diss_reason_4' : ['sum'],
    'diss_reason_5' : ['sum'], 
    'diss_reason_6': ['sum'],
    'diss_reason_7': ['sum'], 
    'diss_reason_8': ['sum'],
    'diss_reason_9': ['sum'],
    'diss_reason_10': ['sum'], 
    'diss_reason_11': ['sum'],
    'diss_reason_12': ['sum'], 
    'diss_reason_14': ['sum'],	
    'diss_reason_13': ['sum'],
    'diss_reason_15': ['sum'],	
    'diss_reason_16' : ['sum'],
   
  }
  ).reset_index()
# %%
df_cnt.columns = ['_'.join(col).strip() for col in df_cnt.columns.values]
df_cnt
# %%
#df_cnt.to_excel('ftds.xlsx')
#%%
dfc = df_cnt.copy()

# %%
dfc['report_month'] =   dfc['report_month_'].astype(str) + ' 01'
dfc['report_month'] = pd.to_datetime(dfc['report_month']) 
# %%
dfc_r = dfc.copy()

#= dfc.query('nps_type_ != "other"')#.to_excel('r.xlsx')

#%%
dfc_r#.columns
#%%
dfc_r['sat_reason_13_sum'] = np.nan
dfc_r
# %%
df_unpivot_s = pd.melt(dfc_r, id_vars=['report_month', 'region_', 'main_operator_', 'nps_type_', 'id_count']
            , value_vars=[  'sat_reason_1_sum', 'sat_reason_2_sum',
            'sat_reason_3_sum', 'sat_reason_4_sum', 'sat_reason_5_sum', 'sat_reason_6_sum',
            'sat_reason_7_sum', 'sat_reason_8_sum', 'sat_reason_9_sum',
            'sat_reason_10_sum', 'sat_reason_11_sum', 'sat_reason_12_sum','sat_reason_13_sum',
            'sat_reason_14_sum', 'sat_reason_15_sum', 'sat_reason_16_sum',
            ],  var_name='sat/diss', value_name='subs')
df_unpivot_s

#%%
df_unpivot_d= pd.melt(dfc_r, id_vars=['report_month', 'region_', 'main_operator_', 'nps_type_', 'id_count',]
            , value_vars=[  'diss_reason_1_sum', 'diss_reason_2_sum', 'diss_reason_3_sum',
            'diss_reason_4_sum', 'diss_reason_5_sum', 'diss_reason_6_sum',
            'diss_reason_7_sum', 'diss_reason_8_sum', 'diss_reason_9_sum',
            'diss_reason_10_sum', 'diss_reason_11_sum', 'diss_reason_12_sum',
            'diss_reason_14_sum', 'diss_reason_13_sum', 'diss_reason_15_sum',
            'diss_reason_16_sum'
            ],  var_name='sat/diss', value_name='subs')
df_unpivot_d
#%%

df_mer = pd.merge(df_unpivot_s, df_unpivot_d[['sat/diss',	'subs']], left_index= True, right_index=True, how='left')
df_mer
#%%
df_mer
#%%
def flfunc(cnt):
  if cnt ['sat/diss_x'] == 'sat_reason_1_sum':
    return "Качество связи"
  elif cnt ['sat/diss_x'] == 'sat_reason_2_sum':
    return "Зона приёма"
  elif cnt ['sat/diss_x'] == 'sat_reason_3_sum':
    return "Тарифы"  
  elif cnt ['sat/diss_x'] == 'sat_reason_4_sum':
    return "Роуминг в поездках за рубеж" 
  elif cnt ['sat/diss_x'] == 'sat_reason_5_sum':
    return "Роуминг в поездках по России" 
  elif cnt ['sat/diss_x'] == 'sat_reason_6_sum':
    return "Платежи" 
  elif cnt ['sat/diss_x'] == 'sat_reason_7_sum':
    return "НЕ/Честность оператора" 
  elif cnt ['sat/diss_x'] == 'sat_reason_8_sum':
    return "Мобильный Интернет" 
  elif cnt ['sat/diss_x'] == 'sat_reason_9_sum':
    return "Абонентская служба" 
  elif cnt ['sat/diss_x'] == 'sat_reason_10_sum':
    return "СМС/ММС" 
  elif cnt ['sat/diss_x'] == 'sat_reason_11_sum':
    return "Мобильное приложение" 
  elif cnt ['sat/diss_x'] == 'sat_reason_12_sum':
    return "НЕ/Навязчивые звонки от оператора" 
  elif cnt ['sat/diss_x'] == 'sat_reason_13_sum':
    return "Навязчивые звонки от представителей банков / интернет-магазинов / других отраслей " 
  elif cnt ['sat/diss_x'] == 'sat_reason_14_sum':
    return "Дополнительные услуги " 
  elif cnt ['sat/diss_x'] == 'sat_reason_15_sum':
    return "Другое" 
  elif cnt ['sat/diss_x'] == 'sat_reason_16_sum':
    return "Затрудняюсь ответить " 
#%%
#df1["nps_type"] = df1.apply(nps_t, axis=1)
df_mer["reasons"] = df_mer.apply(flfunc, axis=1)
df_mer 

#%%
df_mer.query('report_month >= "2021-01-01" and main_operator_ in ("megafon", "beeline", "tele2", "mts" )' ).to_excel('final.xlsx')
#%%
'''
df_unpivot = pd.melt(dfc_r, id_vars=['report_month', 'region_', 'main_operator_', 'nps_type_']
            , value_vars=[ 'id_count', 'sat_reason_1_sum', 'sat_reason_2_sum',
            'sat_reason_3_sum', 'sat_reason_4_sum', 'sat_reason_5_sum', 'sat_reason_6_sum',
            'sat_reason_7_sum', 'sat_reason_8_sum', 'sat_reason_9_sum',
            'sat_reason_10_sum', 'sat_reason_11_sum', 'sat_reason_12_sum',
            'sat_reason_14_sum', 'sat_reason_15_sum', 'sat_reason_16_sum',
            'diss_reason_1_sum', 'diss_reason_2_sum', 'diss_reason_3_sum',
            'diss_reason_4_sum', 'diss_reason_5_sum', 'diss_reason_6_sum',
            'diss_reason_7_sum', 'diss_reason_8_sum', 'diss_reason_9_sum',
            'diss_reason_10_sum', 'diss_reason_11_sum', 'diss_reason_12_sum',
            'diss_reason_14_sum', 'diss_reason_13_sum', 'diss_reason_15_sum',
            'diss_reason_16_sum'],  var_name='sat/diss', value_name='subs')
'''
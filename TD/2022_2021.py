# %%
from functools import reduce

import pandas as pd

dic = {
 'Тарифы (высокие цены, маленький выбор тарифов)':'Тарифы',
 'Роуминг (высокие цены, плохое качество, отсутствует в нужных регионах)':'Роуминг',
 'Платежи (проблемы с зачислением денег, неудобно платить, нельзя звонить при отрицательном балансе)':'Платежи',
 'Мобильный Интернет (плохое качество, низкая скорость, нет 3G, 4G, дорогой)':'Мобильный Интернет',
 'Абонентская служба (не вежливые, плохо работают, мало офисов, не дозвониться)':'Абонентская служба',
 'СМС/ММС (спам, проблемы с доставкой, дорого)':'СМС/ММС',
 'Дополнительные услуги (нет нужных, проблемы с подключением)':'Дополнительные услуги',
 'Мобильное приложение (неудобно пользоваться, мало функций)':'Мобильное приложение',
 '     Роуминг в поездках за рубеж (высокие цены, плохое качество, отсутствует в нужных странах)':'Роуминг в поездках за рубеж',
 '     Роуминг в поездках по России (высокие цены, плохое качество, отсутствует в нужных регионах)':'Роуминг в поездках по России',
 'Мобильный Интернет (плохое качество, низкая скорость, нет 3G, 4G)':'Мобильный Интернет',
 'Навязчивые звонки от оператора с предложением сменить тариф/ подключить услугу':'Навязчивые звонки',
 #detailed reasons
 'Высокая абонентская плата/ Есть абонентская плата':'Высокая абонентская плата/ есть абонентская плата',
 'Высокие цены за соединение/ Есть плата за соединение':'Высокие цены за соединение/ есть плата за соединение',
 'Мобильный Интернет (плохое качество, низкая скорость, нет 3G, 4G)':'Мобильный Интернет',
 'Мобильный Интернет (плохое качество, низкая скорость, нет 3G, 4G, дорогой)':'Мобильный Интернет',
 'Небольшой выбор тарифов/ мало тарифов':'Мало тарифов',
 'Небольшой выбор тарифов/Мало тарифов':'Мало тарифов',
 
 }

dic_region = {'Ростов на Дону':'Ростов-на-Дону',
              'Total Russia new without Moscow':'Total Russia new wo Moscow',
}

dic_Name = {
    'q217a. Что оператор мобильной связи должен улучшить в первую очередь, чтобы Вы ТОЧНО могли его рекомендовать друзьям/знакомым? ВСЕ ОТВЕТЫ. База: поставили 1-8 баллов в q217 или затруднились с ответом ':'Dissatisfied Reasons (NET)',
    'q2171. Плохое качество связи оператора. ВСЕ ОТВЕТЫ. База: выбрали <Плохое качество связи> ':'2171. Плохое качество связи',
    'q2172. Плохая зона приема/покрытие оператора. ВСЕ ОТВЕТЫ. База: выбрали <Плохая зона приема> ':'2172. Плохая зона приема',
    'q2173. Тарифы оператора. ВСЕ ОТВЕТЫ. База: выбрали <Тарифы> ':'2173. Тарифы',
    'q2174. Роуминг оператора. ВСЕ ОТВЕТЫ. База: выбрали <Роуминг> ':'2174. Роуминг',
    'q2175. Платежи оператора. ВСЕ ОТВЕТЫ. База: выбрали <Платежи> ':'2175. Платежи',
    'q2176. Нечестность оператора. ВСЕ ОТВЕТЫ. База: выбрали <Нечестность оператора> ':'2176. Нечестность',
    'q2177. Мобильный интернет. ВСЕ ОТВЕТЫ. База: выбрали <Мобильный интернет> ':'2177. Мобильный интернет',
    'q2178. Работа абонентской службы оператора. ВСЕ ОТВЕТЫ. База: выбрали <Работа абонентской службы> ':'2178. Абонент служба',
    'q2179. СМС/ММС оператора. ВСЕ ОТВЕТЫ. База: выбрали <СМС/ММС> ':'2179. СМС ММС',
}

field_list = [
    'Dissatisfied Reasons (NET)', '21721. Мобильное приложение',
    '2171. Плохое качество связи', '2172. Плохая зона приема',
    '2173. Тарифы', '2174. Роуминг', '2175. Платежи',
    '2176. Нечестность', '2177. Мобильный интернет',
    '2178. Абонент служба', '2179. СМС ММС'
    ]
# %%
df1 = pd.read_excel('./additional request_Dissatisfied_reasons_2lvl.xlsx')
df11 = pd.read_excel('./additional request_Dissatisfied_reasons_2lvl__incl_TR.xlsx')

df = pd.concat([df1, df11])
# %%

df.columns = ['Name', 'Operator', 'Reason', 'Category', 'Region', 'num1', 'num2', 'Extra', "Q1'22", "Q2'22", "Base_Q1'22", "Base_Q2'22"]

df = df.loc[
    (df['Operator'].isin(['Tele2', 'МТС', 'Билайн', 'Мегафон']))&
    (df['num1']=='Subs / nonsubs')&
    (df['Extra'].isin([
        'Рекомендации для оператора мобильной связи. Причины низкой оценки. Base: 1-8; З/О',
        # 'Рекомендации для оператора мобильной связи. Причины низкой оценки. Base: Все абоненты',
        'Мобильное приложение', 'Плохое качество связи',
        'Плохая зона приема', 'Тарифы', 'Роуминг', 'Платежи',
        'Нечестность оператора', 'Мобильный интернет',
        'Абонентская служба', 'СМС/ММС'
        ]))
].reset_index(drop=True)

df = df[['Name', 'Operator', 'Reason', 'Category', 'Region', 'num1', 'num2', 'Extra', "Q1'22", "Q2'22"]]

df = pd.melt(df, 
    id_vars=[
        'Name', 'Operator', 'Reason',
        'Category', 'Region', 'num1',
        'num2', 'Extra'
    ],
    var_name='Quarter',
    value_name='Share'
    )

# %%
u = df['Quarter'].str.replace(r"(Q\d)'(\d+)", r"20\2-\1", regex=True)
df['Quarter'] = pd.to_datetime(u)

df.Reason = df.Reason.replace(dic)
df.Region = df.Region.replace(dic_region)

df.drop(columns=['num1', 'num2'], inplace=True)

# %%
df2 = pd.read_csv('./all.csv', parse_dates=['Quarter'])
df2.Name = df2.Name.replace(dic_Name)



df2 = df2.loc[
    df2['Name'].isin(field_list)
].reset_index(drop=True)
# %%

df_all = pd.concat([df,df2], join='inner')

# %%
df_all.to_csv('2022_2021.csv', index=False)
# %%
pd.lreshape(
    df,
    groups={
        'share':["Q1'22", "Q2'22"],
        'base':["Base_Q1'22", "Base_Q2'22"]
        }
)
# %%
# pd.wide_to_long()
pd.melt(df, 
    id_vars=[
        'Name', 'Operator', 'Reason',
        'Category', 'Region', 'num1',
        'num2', 'Extra'
    ],
    var_name='Quarter',
    value_name='Share',
    value_vars=["Q1'22", "Q2'22"]
    )

# %%
def unpivot(
    df:str,
    ids:str,
    cols:dict
):
    '''
    unpivot from wide to long

    groups={
    'share':["Q1'22", "Q2'22"],
    'base':["Base_Q1'22", "Base_Q2'22"]
    }
    
    ids = ['Name', 'Operator', 'Reason','Category', 'Region', 'num1', 'num2', 'Extra']
    '''

    dfs = []
    for key, value in cols.items():
        melted = pd.melt(
            df, 
            id_vars=ids,
            var_name='Quarter',
            value_name=key,
            value_vars=value
            )
        dfs.append(melted)
    
    final_df = reduce(lambda  left,right: pd.merge(left, right, on=ids,
                                            how='outer'), dfs)
    return final_df

# %%
groups={
    'share':["Q1'22", "Q2'22"],
    'base':["Base_Q1'22", "Base_Q2'22"]
    }
ids = ['Name', 'Operator', 'Reason','Category', 'Region', 'num1', 'num2', 'Extra']

dff = unpivot(df, ids=ids, cols=groups)
# %%

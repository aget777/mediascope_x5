#!/usr/bin/env python
# coding: utf-8

# In[1]:


import gspread
from oauth2client.service_account import ServiceAccountCredentials
import pandas as pd

import config
import config_tv_index
import config_media_costs
from db_funcs import get_mssql_table



# In[2]:


"""
функция для того, чтобы создать подключение к Гугл докс
"""

def create_connection(service_file):
    client = None
    scope = [
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/drive.file",
    ]
    try:
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            service_file, scope
        )
        client = gspread.authorize(credentials)
        print("Connection established successfully...")
    except Exception as e:
        print(e)
    return client


# In[3]:


"""
функция для загрузки данных в гугл таблицу
"""

def export_dataframe_to_google_sheet(worksheet, df):
    try:
        worksheet.update(
            [df.columns.values.tolist()] + df.values.tolist(),

        )
        print("DataFrame exported successfully...")
    except Exception as e:
        print(e)


# In[4]:


"""
с помощью этой функции делаем выгрузку новых объявлений на отдельный лист 
эту функцию запускаем в самом конце после обновлений всех таблиц
она затирает все данные, которые были на листе и записывает заново только новые объявления
"""

def append_ads_to_google(query='', worksheet=''):
    if query and worksheet:
        # делаем запрос к БД, чтобы получить новые объявления
        nat_tv_new_ad_dict_df = get_mssql_table(config.db_name, query=query)
        # создаем подключение к гуглу
        client = create_connection(config.service)
        # прописываем доступы к документу, в который будем вносить запись
        sh = client.open_by_url(config.google_docs_link)
        sh.share(config.gmail, perm_type='user', role='writer')
        google_sheet = sh.worksheet(worksheet)
        # очищаем лист
        google_sheet.clear()
        # записываем новые данные
        export_dataframe_to_google_sheet(google_sheet, nat_tv_new_ad_dict_df)
    else:
        print('Передайте параметры запроса / Название листа для сохранения данных')


# In[5]:


# append_ads_to_google()


# In[8]:


# ratings / media
def query_for_google(report='ratings'):
    df = pd.read_csv(config.full_cleaning_link,  skiprows=[0], nrows=1)
    df_cols = list(df.columns)

    if report=='ratings':
        db_cols_dict = config_tv_index.google_new_ads_nat_tv['db_cols']
        db_query = config_tv_index.google_new_ads_nat_tv['query']
    if report=='media':
        db_cols_dict = config_media_costs.google_new_ads_media_invest['db_cols']
        db_query = config_media_costs.google_new_ads_media_invest['query']

    tmp_lst = []
    for key, value in db_cols_dict.items():
        if key in df_cols:
            tmp_lst.append(value)

    query = 'select ' + ','.join(tmp_lst) + ' ' + db_query

    return query


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import re
import sys
import json
import gc
import warnings
from datetime import datetime, date, timedelta


import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import turbodbc
from turbodbc import connect

from IPython.display import JSON
from mediascope_api.core import net as mscore
from mediascope_api.mediavortex import tasks as cwt
from mediascope_api.mediavortex import catalogs as cwc

# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

import config
import config_tv_index
import config_reg_tv
from normalize_funcs import normalize_columns_types, get_cleaning_dict
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table


db_name = config.db_name


# In[2]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)


# In[3]:


"""
функция для получения справочников из ТВ индекс
на входе принимает 
- название справочника из нашей БД mssql
- список ИД для загрузки
Мы забираем к себе в БД НЕ все, что есть в ТВ индексе. Нам нужны только те ИД и Названия, категорий, которые встречаются в нашей БД
поэтому search_list - это список ИД, которые есть в нашей таблице фактов, но отсутствуют в справочнике
- при первой загрузке наши справочники пустые, поэтому в search_lst попадут ВСЕ ИД, котрые нам нужны
"""

def get_tv_index_dicts(dict_name, search_lst=None):
    if search_lst:
        search_lst = [str(id) for id in search_lst]

    if 'advertiserList' in dict_name:
        df = cats.get_tv_advertiser_list(search_lst)
        df[['name', 'ename']]
        df = df.rename(columns={'id': 'advertiserListId', 'name': 'advertiserListName', 'ename': 'advertiserListEName'})

    if 'brandList' in dict_name:
        df = cats.get_tv_brand_list(search_lst)
        df = df.rename(columns={'id': 'brandListId', 'name': 'brandListName', 'ename': 'brandListEName'})

    if 'subbrandList' in dict_name:
        df = cats.get_tv_subbrand_list(search_lst)
        df = df.rename(columns={'id': 'subbrandListId', 'name': 'subbrandListName', 'ename': 'subbrandListEName'})

    if 'modelList'in dict_name:
        df = cats.get_tv_model_list(search_lst)
        df = df.rename(columns={'id': 'modelListId', 'name': 'modelListName', 'ename': 'modelListEName'})

    if 'articleList2' in dict_name:
        df = cats.get_tv_article_list2(search_lst)
        df = df.rename(columns={'id': 'articleList2Id', 'name': 'articleList2Name', 'ename': 'articleList2EName'})

    if 'articleList3' in dict_name:
        df = cats.get_tv_article_list3(search_lst)
        df = df.rename(columns={'id': 'articleList3Id', 'name': 'articleList3Name', 'ename': 'articleList3EName'})

    if 'articleList4' in dict_name:
        df = cats.get_tv_article_list4(search_lst)
        df = df.rename(columns={'id': 'articleList4Id', 'name': 'articleList4Name', 'ename': 'articleList4EName'})

    # if 'adId' in dict_name:
    #     df = cats.get_tv_ad(search_lst)
    #     df = df.rename(columns={'id': 'adId', 'name': 'adName', 'ename': 'adEName'})

    if 'adSloganAudioId' in dict_name:
        df = cats.get_tv_ad_slogan_audio(search_lst)
        df = df.rename(columns={'id': 'adSloganAudioId', 'name': 'adSloganAudioName', 'notes': 'adSloganAudioNotes'})

    if 'adSloganVideo' in dict_name:
        df = cats.get_tv_ad_slogan_video(search_lst)
        df = df.rename(columns={'id': 'adSloganVideoId', 'name': 'adSloganVideoName', 'notes': 'adSloganVideoNotes'})

    if 'region' in dict_name:
        df = cats.get_tv_region(search_lst)
        df = df.rename(columns={'id': 'regionId', 'name': 'regionName', 'ename': 'regionEName'})

    if 'tvNet' in dict_name:
        df = cats.get_tv_net()
        df = df.rename(columns={'id': 'tvNetId', 'name': 'tvNetName', 'ename': 'tvNetEName'})
        df['nid_custom'] = 'tv' + '_' + df['tvNetId'].astype(str)

    if 'tvCompany' in dict_name:
        df = cats.get_tv_company(search_lst)
        df = df.rename(columns={'id': 'tvCompanyId', 'name': 'tvCompanyName', 'ename': 'tvCompanyEName'})
        df['cid_custom'] = 'tv' + '_' + df['tvCompanyId'].astype(str)
        df['nid_custom'] = 'tv' + '_' + df['tvNetId'].astype(str)

    if 'adType' in dict_name:
        df = cats.get_tv_ad_type(search_lst)
        df = df.rename(columns={'id': 'adTypeId', 'name': 'adTypeName', 'ename': 'adTypeEName'})
        df['ad_type_custom'] = 'tv' + '_' + df['adTypeId'].astype(str)

    if 'adDistributionType' in dict_name:
        df = cats.get_tv_breaks_distribution()
        df = df.rename(columns={'id': 'adDistributionType'})

    return df


# In[4]:


"""
функция для обновления всех словарей ТВ индекс КРОМЕ nat_tv_ad_dict
После записи данных в отчеты Simple и Buying
в справочнике nat_tv_ad_dict - содержится cleanig_flag, который еше НЕ обновился
таким образом у нас зафиксировано состояние с прошлой загрузки и мы можем понять, какие новые объявления появилсь в БД
для этого по каждому отдельному столбцу, который является ключом для верхнеуровневых справочников
мы забираем список уникальных ИД, у которых cleanig_flag=2 (т.е. только новые объявления)
список этих ИД мы передаем в запрос к ТВ Индексу, чтобы только то, что нам нужно
в конце жобавляем новые строки к справочникам
"""

def update_tv_index_dicts():

    # сначала проверяем справочник объявлений - есть там новые или нет
    query = f"select top(1) adId from nat_tv_ad_dict where cleaning_flag=2"
    df = get_mssql_table(config.db_name, query=query)

    # если ответ НЕ пустой, то запускаем логику обновления всех верхнеуровневых справочников
    if not df.empty:
        # у нас сформирован справочник словарей
        # Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
        # Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
        for key, value in config_tv_index.tv_index_dicts.items():
            # передаем в SQL запрос название поля, которое нас интересует
            query = f"select distinct {key} from nat_tv_ad_dict"
            # отправляем запрос в Общий справочник объявлений
            ad_dict= get_mssql_table(config.db_name, query=query)
            # переводим ИД в список
            ad_dict = ad_dict[key].tolist()
            # Готовим запрос к отдельному справочнику, по котором хотим сделать обновление
            query = f"select distinct {key} from {value[0]}"
            target_dict= get_mssql_table(config.db_name, query=query)
            # переводим ИД в список
            target_dict = target_dict[key].tolist()

            # теперь мы хотим узнать в Общем справочнике nat_tv_ad_dict
            # есть ли какие-то ИД, которых НЕТ в другом
            search_lst = list(set(ad_dict) - set(target_dict))
            # если такие ИД есть, то передаем их на загрузку характеристик из ТВ Индекса
            if search_lst:
                # отправляем запрос в ТВ индекс
                df = get_tv_index_dicts(key, search_lst)
                # нормализуем данные
                df = normalize_columns_types(df, value[2])
                # записываем в БД
                downloadTableToDB(db_name, value[0], df)
    else:
        print(f'Новых данных для загрузки нет')


# In[ ]:





# In[5]:


"""
с помощью этой функции мы создаем и заполняем словари из ТВ Индекс
есть набор некоторы таблиц, которые НЕ связаны с ИД объявления
соответсвенно их нет в справочнике nat_tv_ad_dict
поэтому при первой загрузке данных мы создаем и сразу заполняем такие справочники
в дальнейшешем их НЕ нужно обновлять, т.е. они хранятся в неизменном виде
список справочников по умолчанию указан в этом словаре tv_index_default_dicts
"""

def download_tv_index_default_dicts():
    for key, value in config_tv_index.tv_index_default_dicts.items():
        # создаем пустые таблицы для словарей по умолчанию
        # createDBTable(db_name, value[0] , value[1], flag='create')
        # если таблица уже существовала, то обнуляем в ней данные
        # createDBTable(db_name, value[0], value[1], flag='drop')
        # отправляем запрос в ТВ индекс
        df = get_tv_index_dicts(key)
        # нормализуем данные
        df = normalize_columns_types(df, value[2])
        # записываем в БД
        # если таблица уже существовала, то обнуляем в ней данные
        createDBTable(db_name, value[0], value[1], flag='drop')
        downloadTableToDB(db_name, value[0], df)


# In[6]:


# """
# функция для обонвления основного справочнка объявлений nat_tv_ad_dict
# ее запускаем в самую после заливки данных из ТВ индекс и обновления всех справочников
# НО ПЕРЕД заливкой новых объявлений в гугл докс
# """

# def update_nat_tv_ad_dict(media_type='tv'):
#     # забираем гугл докс с чисткой
#     df_cleaning_dict = get_cleaning_dict(media_type)

#     media_type_short = media_type.upper()
#     # df_cleaning_dict['media_key_id'] = media_type + '_' + df_cleaning_dict['adId'].astype('str')
#      # нормализуем типы данных
#     df_cleaning_dict = normalize_columns_types(df_cleaning_dict, config.custom_ad_dict_int_lst) 

#     # создаем список из названий полей, которые нам нужны дальше для метчинга
#     custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
#     custom_cols_list = list(set(custom_cols_list) - set(['adId', 'media_type', 'media_type_long']))
#     # оставляем только нужные поля
#     df_cleaning_dict = df_cleaning_dict[custom_cols_list]

#     # формируем список названий полей, которые нам нужно забрать из БД
#     # из справочника nat_tv_ad_dict
#     nat_tv_ad_dict_short_cols = [col[:col.find(' ')] for col in config_tv_index.nat_tv_ad_dict_vars_list]
#     nat_tv_ad_dict_short_cols = list(set(nat_tv_ad_dict_short_cols) - set(custom_cols_list)) + ['media_key_id']
#     # приводим список к строке
#     nat_tv_ad_dict_short_cols = ', '.join(nat_tv_ad_dict_short_cols)

#     # отправляем запрос в БД и забираем ВСЕ строки и нужные поля
#     query = f"select {nat_tv_ad_dict_short_cols}  from {config_tv_index.nat_tv_ad_dict}"
#     nat_tv_ad_dict_df = get_mssql_table(db_name, query=query) 

#     # объединяем справочник из БД с таблицей чистки
#     nat_tv_ad_dict_df = nat_tv_ad_dict_df.merge(df_cleaning_dict, how='left', left_on=['media_key_id'], right_on=['media_key_id'])
#     # ИД объявлений, которые НЕ нашли сопосталвения, мы считаем новыми и присваимаем им флаг=2
#     nat_tv_ad_dict_df['cleaning_flag'] = nat_tv_ad_dict_df['cleaning_flag'].fillna(2)
#     # остальные NaN заполняем пустотой
#     nat_tv_ad_dict_df = nat_tv_ad_dict_df.fillna('')
#     # создаем список полей, которые нужно оставить в этом датаФрейме
#     nat_tv_ad_dict_cols = [col[:col.find(' ')] for col in config_tv_index.nat_tv_ad_dict_vars_list]
#     nat_tv_ad_dict_df = nat_tv_ad_dict_df[nat_tv_ad_dict_cols]
#     # нормализуем типы данных
#     nat_tv_ad_dict_df = normalize_columns_types(nat_tv_ad_dict_df, config_tv_index.nat_tv_ad_dict_int_lst)

#     # удаляем все данные из справочника nat_tv_ad_dict в БД 
#     createDBTable(db_name, config_tv_index.nat_tv_ad_dict, config_tv_index.nat_tv_ad_dict_vars_list, flag='drop')

#     # записываем новые данные в справочник Объявлений
#     downloadTableToDB(db_name, config_tv_index.nat_tv_ad_dict, nat_tv_ad_dict_df)


# In[7]:


"""
функция для обонвления основного справочнка объявлений nat_tv_ad_dict
ее запускаем в самую после заливки данных из ТВ индекс и обновления всех справочников
НО ПЕРЕД заливкой новых объявлений в гугл докс
"""

def update_nat_tv_ad_dict(media_type='tv', tv_report='nat'):
    # забираем гугл докс с чисткой
    df_cleaning_dict = get_cleaning_dict(media_type)

    media_type_short = media_type.upper()
    # df_cleaning_dict['media_key_id'] = media_type + '_' + df_cleaning_dict['adId'].astype('str')
     # нормализуем типы данных
    df_cleaning_dict = normalize_columns_types(df_cleaning_dict, config.custom_ad_dict_int_lst) 

    # создаем список из названий полей, которые нам нужны дальше для метчинга
    custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
    custom_cols_list = list(set(custom_cols_list) - set(['adId', 'media_type', 'media_type_long']))
    # оставляем только нужные поля
    df_cleaning_dict = df_cleaning_dict[custom_cols_list]

    if tv_report=='nat':
        # забираем список полей, которые созданы в БД в таблице фактов Simple
        table_name = config_tv_index.nat_tv_ad_dict
        vars_cols_lst = config_tv_index.nat_tv_ad_dict_vars_list
        int_lst = config_tv_index.nat_tv_ad_dict_int_lst

    if tv_report=='reg':
        # забираем список полей, которые созданы в БД в таблице фактов Simple
        table_name = config_reg_tv.reg_tv_ad_dict
        vars_cols_lst = config_reg_tv.reg_tv_ad_dict_vars_list
        int_lst = config_reg_tv.reg_tv_ad_dict_int_lst

    # формируем список названий полей, которые нам нужно забрать из БД
    # из справочника nat_tv_ad_dict
    df_ad_dict_short_cols = [col[:col.find(' ')] for col in vars_cols_lst]
    df_ad_dict_short_cols = list(set(df_ad_dict_short_cols) - set(custom_cols_list)) + ['media_key_id']
    # приводим список к строке
    df_ad_dict_short_cols = ', '.join(df_ad_dict_short_cols)

    # отправляем запрос в БД и забираем ВСЕ строки и нужные поля
    query = f"select {df_ad_dict_short_cols}  from {table_name}"
    df_ad_dict = get_mssql_table(db_name, query=query) 

    # объединяем справочник из БД с таблицей чистки
    df_ad_dict = df_ad_dict.merge(df_cleaning_dict, how='left', left_on=['media_key_id'], right_on=['media_key_id'])
    # ИД объявлений, которые НЕ нашли сопосталвения, мы считаем новыми и присваимаем им флаг=2
    df_ad_dict['cleaning_flag'] = df_ad_dict['cleaning_flag'].fillna(2)
    # остальные NaN заполняем пустотой
    df_ad_dict = df_ad_dict.fillna('')
    # создаем список полей, которые нужно оставить в этом датаФрейме
    df_ad_dict_cols = [col[:col.find(' ')] for col in vars_cols_lst]
    df_ad_dict = df_ad_dict[df_ad_dict_cols]
    # нормализуем типы данных
    df_ad_dict = normalize_columns_types(df_ad_dict, int_lst)

    # удаляем все данные из справочника nat_tv_ad_dict в БД 
    createDBTable(db_name, table_name, vars_cols_lst, flag='drop')

    # записываем новые данные в справочник Объявлений
    downloadTableToDB(db_name, table_name, df_ad_dict)
    # return df_ad_dict


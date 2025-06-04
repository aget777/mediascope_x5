#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np

import config
import config_tv_index
import config_media_costs
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table
from config_search_funcs import get_outdoor_regions


regions_id = get_outdoor_regions()
np.object = np.object_

# db_name = config.db_name
# ссылка на гугл csv Словарь чистки объявлений
# full_cleaning_link = config.full_cleaning_link


# In[2]:


"""
функция забирает гугл докс с чисткой Объявлений
можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
по умолчанию забираем все
"""

def get_cleaning_dict(media_type='tv'):
    media_type = media_type.upper()
    # т.к. забираем csv по ссылке, чтобы исключить ошибки при добавлении новых столбцов
    # формируем список из номеров колонок от 0 до 45
    cols_count = [i for i in range(45)]
    # Нужные нам заголовки находятся в строке с индексом 1. Поэтому строку с индексом 0 пропускаем
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(config.full_cleaning_link, header=None,  skiprows=[0], usecols=cols_count, low_memory=False)
    # поднимаем из 1-ой строки названия в заголовки
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df['media_type'] = df['media_type'].str.upper()
    df = df.fillna('')

    df = df[df['media_type']==media_type]
    # на всякий случай пересоздаем ключ для связи с таблицами из Рейтингов и Расходов
    df['media_key_id'] = df['media_type'] + '_' + df['adId'].astype('str')
# удаляем дубликаты
    df = df.drop_duplicates('media_key_id')
# создаем флаг для очищенных / удаленных объявлений
    df['cleaning_flag'] = df['include_exclude'].apply(lambda x: 1 if x=='include' else 0)
    use_cols = [i[:i.find(' ')] for i in config.custom_ad_dict_vars_list if i[:i.find(' ')] in list(df.columns)]
    df = df[use_cols]
    # чтобы избежать ошибок из-за слишком длинных строк, приводим их длину в соответсвии с нормой
    # параметры указаны в словаре config.custom_ad_dict_vars_list
    # для полей с типом данных Текст, создаем словарь, где ключ - это название поля / значение - это длина строки
    len_text_dict = {i[:i.find(' ')]: int(i[i.find('(')+1: i.find(')')]) for i in config.custom_ad_dict_vars_list if 'nvarchar' in i}
    # перебираем наш словарь
    for key, value in len_text_dict.items():
        # если поле есть в датаФрейме, то применяем правила 
        if key in df.columns:
            df[key] = df[key].str.slice(0, value)

# приводим строки в верхний регистр, нормализуем цифры и тд.
    custom_ad_dict_int_lst = config.custom_ad_dict_int_lst
    df = normalize_columns_types(df, custom_ad_dict_int_lst)

    return df


# In[12]:





# In[ ]:





# In[5]:


"""
создаем функцию для получения Дисконтов по типам медиа
можем передать в нее список с названиями типов медиа, по которым отфильтровать датаФрейм
по умолчанию забираем все
"""

def get_media_discounts(media_type='tv'):
    media_type = media_type.upper()
    cols_count = [i for i in range(3)]
    # опускаем заголовки на 1-ую строку датаФрейма и передаем номера столбцов, которые нам нужны
    df = pd.read_csv(config.discounts_link, header=None, usecols=cols_count)
    df = df.rename(columns=df.iloc[0]).drop(df.index[0])
    df = df[df['media_type']==media_type]
    df = normalize_columns_types(df, ['year'], ['disc'])

    return df


# In[ ]:





# In[ ]:





# In[6]:


"""
функция для нормализации данных - приводим в нижний регистр, заполняем пропуски и округляем до 2-х знаков после запятой
принимает на вход:
- датаФрейм
- список из названий полей с типом данных Int (по умолчанию пустой список)
- список из названий полей с типом данных Float (по умолчанию пустой список)
"""

def normalize_columns_types(df, int_lst=list(), float_lst=list()):
    varchar_lst = list(df.columns) #df.loc[:,df.dtypes==np.object].columns # Через всторенный метод находим поля с текстовыми данными
    varchar_lst = list(set(varchar_lst) - set(int_lst) - set(float_lst)) # исключаем из списка с текстовыми данными поля Int и Float
    df[varchar_lst] = df[varchar_lst].apply(lambda x: x.astype('str').str.upper().str.strip())



    # Обрабатываем поля с типом данных Int
    df[int_lst] = df[int_lst].fillna('0')
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', ''))
    df[int_lst] = df[int_lst].apply(lambda x: x.astype('float64').astype('int64'))
    # Обрабатываем поля с типом данных Float
    df[float_lst] = df[float_lst].fillna('0.0')
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('str').str.replace('\xa0', '').str.replace(',', '.').str.replace(' ', '').str.replace('р.', ''))
    df[float_lst] = df[float_lst].apply(lambda x: x.astype('float64').round(2))

# возвращаем нормализованный датаФрейм
    return df


# In[ ]:





# In[7]:


"""
функция для добавления флага чистки
на вход принимает основной датаФрейм
и сокращенный датаФрейм из гугл диска, в котором оставили только ИД объявления и флаг чистки
если объявления нет в гуглдоксе, то ставим флаг 2
report - тип отчета 
base - выполняется для всех. просто добавляем media_key_id и другие кастомные ключи (если они есть)
simple - добавляем флаг чистки
buying - добавляем дисконт к расходам
"""

def append_custom_columns(df, report, nat_tv_ad_dict=None, media_type='tv'):
    # здесь записываем сокращенное название медиа tv-ra-pr-od
    df['media_type'] = media_type.upper()
    # забираем полное название
    media_type = config.media_type_full_dict[media_type.lower()].upper()
    # добавляем поле с типом мелиа - TV
    # создаем спец ключ для объединения со справочником чистка объявлений
    df['media_type_long'] = media_type
    # для отчетов Фактов в таблицах передается тип дистрибуции (Локальный, Орбитальный и тд)
    # так же передается тип объявления (Спонсор, Ролик и тд)
    # с помощью этих полей мы можем определить более расширенный тип Медиа (Нат тв, Рег тв и тд)
    # в справочниках объявлений НЕТ понятия Дистрибуция, т.к. один и тот же ролик может быть и Локальным и Орбитальным и тд
    if report.lower() in ('buying', 'simple'):
        df['media_type_detail'] = df.apply(getMediaTypeDetail,  media_type=media_type, axis=1)

    if 'vid' in list(df.columns):
        df = df.rename(columns={'vid': 'adId'})

    if 'adId' in list(df.columns):
        df['media_key_id'] = df['media_type_long'] + '_' + df['adId'].astype('str')

    # для таблиц из Медиаскоп в поля cid, nid, atid необходимо добавить префикс с названием медиа
    # чтобы некторые талицы можно было объединить в одну
    for key, value in config.custom_cols_dict.items():
        if key in list(df.columns):
            df[value] = media_type + '_' + df[key].astype(str)

    if report.lower()=='buying':
        # забираем таблицу Медиа дисконтов
        media_discounts = get_media_discounts(media_type)
        media_discounts = media_discounts.rename(columns={'media_type': 'media_type_tmp'})
    # забираем Год из даты отчета, чтобы добавить дисконт
        df['year'] = df['researchDate'].str.slice(0, 4)
        df['year'] = df['year'].astype('int64')

        df = df.merge(media_discounts, how='left', left_on=['media_type_long', 'year'], right_on=['media_type_tmp', 'year'])
        df['ConsolidatedCostRUB_disc'] = df['ConsolidatedCostRUB'] - df['ConsolidatedCostRUB']* df['disc']


    if report.lower()=='simple':
        # проверяем - является эта переменная ДатаФреймом или нет
        # эта проверка нужна для резервного сценария Обновить в таблицах Рейтингов значения, которые сами присваиваем
        if isinstance(nat_tv_ad_dict, pd.DataFrame):
            # добавляем флаг чистки в датаФрейм
            df = df.merge(nat_tv_ad_dict, how='left', left_on=['media_key_id'], right_on=['media_key_id'])
            # ставим флаг чистки = 2 для ИД новый неочищенных объявлений
            df['cleaning_flag'] = df['cleaning_flag'].fillna(2)
            df = df.fillna('')

    if report.lower()=='ad':
        df_cleaning_dict = get_cleaning_dict(media_type)
        custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
        custom_cols_list = list(set(custom_cols_list) - set(['adId', 'media_type', 'media_type_long']))
        # оставляем только нужные поля
        df_cleaning_dict = df_cleaning_dict[custom_cols_list]
        # объединяем справочник из БД с таблицей чистки
        df = df.merge(df_cleaning_dict, how='left', left_on=['media_key_id'], right_on=['media_key_id'])
        # ставим флаг чистки = 2 для ИД новый неочищенных объявлений
        df['cleaning_flag'] = df['cleaning_flag'].fillna(2)
        df = df.fillna('')

    return df


# In[8]:


def getMediaTypeDetail(row, media_type='tv'):
    media_type = media_type.lower()
    atid = int(row['adTypeId'])
    distr = row['adDistributionType'].upper()
    if  media_type=='outdoor':
        rid =  int(row['rid'])
# если ИД города находится в списке, то присваиваем типу медиа новое название
        if rid in regions_id:
            return 'Outdoor_new_cites'
        else:
            return 'Outdoor'

    if  media_type=='press':
        return media_type

    if  media_type=='radio':
        return media_type

        # здесь просто руками переписал условия из гугл докс
    if  media_type=='tv':

        if (distr=='O' and atid==1) or (distr=='N' and atid==1):
            return 'tv_nat'

        if  distr=='L' and atid==1:
            return 'tv_reg'

        if (distr=='L' and  atid in (23, 25)) or (distr=='N' and  atid in (5, 23, 24, 25)):
            return 'tv_spon'

        else:
            return 'tv'


# In[9]:


"""
- если media_type=tv прописываем adTypeName 
- если media_type=ooh и region=МОСКВА пишем МОСКВА
- если media_type=ooh и  region=МОСКОВСКАЯ ОБЛАСТЬ пишем МОСКОВСКАЯ ОБЛАСТЬ
"""

def get_tv_type_ooh_reg(df, media_type='tv', report='ratings'):
    media_type = media_type.upper()
    if media_type=='TV':
         # формируем запрос к справочнику объявлений, из которого нам нужно достать характеристику
        # Забираем из справочник Тип объявления из таблиц ТВ Индекс
        if report.lower()=='ratings':
            query = f"select adTypeId, adTypeName from tv_index_ad_type_dict"
        # Забираем из справочник Тип объявления из таблиц Медиаскоп
        if report.lower()=='media':
            query = f"select adTypeId, adTypeName from adex_ad_type_dict_tv"

        ad_type_df = get_mssql_table(config.db_name, query=query)
        df['adTypeId'] = df['adTypeId'].astype('int64')
        ad_type_df['adTypeId'] = ad_type_df['adTypeId'].astype('int64')
        df = df.merge(ad_type_df, how='left', left_on=['adTypeId'], right_on=['adTypeId'])
        df = df.rename(columns={'adTypeName': 'tv_type_ooh_reg'})

        return df

    if media_type=='OUTDOOR':
        tmp_dict = {1: 'МОСКВА', 123: 'МОСКОВСКАЯ ОБЛАСТЬ'}
        df['tv_type_ooh_reg'] = df['rid'].apply(lambda x: tmp_dict[x] if x in (1, 123) else '')

    else:
        df['tv_type_ooh_reg'] = ''

    return df


# In[ ]:





# In[ ]:





# In[ ]:





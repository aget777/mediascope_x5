#!/usr/bin/env python
# coding: utf-8

# In[1]:


import warnings

import pandas as pd
import numpy as np
from sqlalchemy import create_engine
import pyodbc
import turbodbc
from turbodbc import connect


import config
import config_media_costs
import config_reg_tv

from normalize_funcs import normalize_columns_types, append_custom_columns, get_cleaning_dict
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table, get_mssql_russian_chars, createView


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

sep_str = '*' * 50


# In[ ]:





# In[ ]:





# In[12]:





# In[3]:


"""
При первой загрузке!
создаем пустые словари справочников ИД Объявлений adex_ad_dict_list_{media_type} и adex_ad_appendix_dict_{media_type}
через цикл забираем из файла create_dicts - словарь, где 
ключ - это название таблицы в Медиаскоп (для дальнейшего удобства так сделано)
значение - это список, который содержит:
[0] - название таблицы в нашей БД
[1] - список полей с типами данных для БД
[2] - список полей с целочисленными значениями для нормализации
[3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
[4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
[5] - поля, которые запрашиваем в Медиаскопе
"""
def create_adex_tables():

# создаем справочники по умолчанию, которые используются для ВСЕХ типов медиа, т.е. являются общими
    for key, value in config_media_costs.adex_defult_dicts.items():
        table_name = value[0]
        vars_list = value[1]
        createDBTable(config.db_name, table_name, vars_list, flag='drop')

# создаем пустые справочники для каждого типа медиа
    for media_type in config.media_type_full_dict.keys():

        # создаем пустые таблицы фактов для каждого типа медиа media_tv_costs/ media_radio_costs / media_outdoor_costs / media_press_costs
        if media_type in config_media_costs.media_dicts_costs:
            table_name = config_media_costs.media_dicts_costs[media_type][0]
            vars_list = config_media_costs.media_dicts_costs[media_type][1]
            createDBTable(config.db_name, table_name, vars_list, flag='create')

        # Здесь уникальные справочники которые НЕ требуют обновления. Например Название ТВ-каналов, Радио станций и тд
        if media_type in config_media_costs.adex_unique_defult_media_dicts:
            tables_dict = config_media_costs.adex_unique_defult_media_dicts[media_type]
            for key, value in tables_dict.items():
                table_name = tables_dict[key][0]
                vars_list = tables_dict[key][1]
                createDBTable(config.db_name, table_name, vars_list, flag='drop')

# Здесь уникальные справочники которые ТРЕБУЮТ обновления. Например tv_Ad_slogan_audio
        if media_type in config_media_costs.adex_unique_media_dicts:
            tables_dict = config_media_costs.adex_unique_media_dicts[media_type]
            for key, value in tables_dict.items():
                table_name = tables_dict[key][0]
                vars_list = tables_dict[key][1]
                createDBTable(config.db_name, table_name, vars_list, flag='drop')

# Здесь справочники ИД объявлений. Например tv_Ad, tv_Appendix 
        if media_type in config_media_costs.adex_ad_dicts:
            tables_dict = config_media_costs.adex_ad_dicts[media_type]
            for key, value in tables_dict.items():
                table_name = tables_dict[key][0]
                vars_list = tables_dict[key][1]
                createDBTable(config.db_name, table_name, vars_list, flag='drop')

# создаем Общие справочники List. Например - BrandList, ModelList, ArticleList2 и тд.
    for key, value in config_media_costs.adex_all_media_list_dicts.items():
        createDBTable(config.db_name, value[0], value[1], flag='drop')

    # # создаем Общие справочники Level. Например - Brand, Model, Article и тд.
    # for key, value in config_media_costs.adex_all_media_level_dicts.items():
    #     if key!='Article':
    #         createDBTable(config.db_name, value[0] , value[1], flag='create')
    #     else:
    #         for sub_key, sub_value in value.items():
    #             createDBTable(config.db_name, sub_value[0] , sub_value[1], flag='create')



# In[ ]:





# In[4]:


"""
Функция для загрузки справочников по умолчанию НЕ ОБНОВЛЯЕМ
Для каждого типа медиа у нас существуют словари, которые НЕ требуют обновления
Их заполняем один раз при первой загрузке / в дальнейшем обновление НЕ нужно
данная функция на вход принимает тип медиа (tv, radio, outdoor, press)
и забирает для каждого типа медиа вложенный словарь с набором таблиц и их характеристик для загрузки в БД
у нас есть 2 типа таких справочников 
- Общие, которые НЕ зависят от типа медиа, например справочник Регионов (их заполняем данными без условий)
- Уникальные - используются только в отдельном медиа (например, справочник ТВ каналов) - в этом случае заполняем справоник
данными только в том случае, если по этому типу медиа мы забираем статистику
Пример таблиц
BreakDistr / EState / Region
tv_Company -> adex_company_dict_tv / tv_Ad_type -> adex_ad_type_dict_tv
ra_Station -> adex_company_dict_radio / ra_Ad_type -> adex_ad_type_dict_radio
od_Agency -> adex_company_dict_outdoor / od_Network -> adex_network_dict_outdoor
pr_Edition -> adex_company_dict_press / pr_Ad_type -> adex_ad_type_dict_press
"""
def download_adex_default_dicts():
    # Общие справочники, которые подходят для всех типов Медиа
    # Загружаем только 1 раз при первой загрузке 
    # НЕОБНОВЛЯЕМ!
    for key, value in config_media_costs.adex_defult_dicts.items():
        table_name = value[0]
        vars_list = value[1]
        int_lst = value[2]
        mediascope_fields_name = value[5]
        # забираем данные из Медиаскоп
        # на этом этапе внутри функции добавляем кастомные поля, если они нужны 
        # нормализуем типы данных
        df = get_adex_tables(mediascope_table=key, mediascope_fields_name=mediascope_fields_name, int_lst=int_lst)
        # забираем список полей, которые созданы в БД
        vars_colst_lst = [col[:col.find(' ')] for col in vars_list]
        # оставляем только нужные поля в датаФрейме для загрузки в БД
        df = df[vars_colst_lst]
        # если таблица уже существовала, то обнуляем в ней данные
        createDBTable(db_name, table_name, vars_list, flag='drop')
        # загружаем в нашу БД
        downloadTableToDB(config.db_name, table_name, df)

    # проходим по типам медиа, по которым забираем статистику
    for media_type, media_type_long in config.media_type_dict.items():
        # проверяем есть ли словарь со справочниками для загрузки по этому типу медиа
        if media_type in config_media_costs.adex_unique_defult_media_dicts:
            # если такой словарь есть, то забираем его
            tables_dict = config_media_costs.adex_unique_defult_media_dicts[media_type]

            # Ключ - это название таблицы, которую нужно забрать из Медиаскопа при Первой загрузке
            # значение - это список, который содержит:
            # [0] - название таблицы в нашей БД
            # [1] - список полей с типами данных для нашей БД mssql
            # [2] - список полей с целочисленными значениями для нормализации
            # [3] - поле, по которому выполняется фильтрация в исходной БД Медиаскоп при ОБНОВЛЕНИИ справочника
            # [4] - поле в нашей БД, по которому проверяем наличие новых ИД в таблицах Фактов
            # [5] - поля, которые запрашиваем в Медиаскопе
            for key, value in tables_dict.items():
                table_name = tables_dict[key][0]
                vars_list = tables_dict[key][1]
                int_lst = tables_dict[key][2]
                mediascope_fields_name = tables_dict[key][5]
                # обращаемся в Медиаскоп для получения нужной таблицы
                # на этом этапе внутри функции добавляем кастомные поля, если они нужны 
                # нормализуем типы данных

                df = get_adex_tables(mediascope_table=key, mediascope_fields_name=mediascope_fields_name, media_type=media_type, int_lst=int_lst)
                # забираем список полей, которые созданы в БД
                vars_colst_lst = [col[:col.find(' ')] for col in vars_list]
                # оставляем только нужные поля в датаФрейме для загрузки в БД
                df = df[vars_colst_lst]
                # если таблица уже существовала, то обнуляем в ней данные
                createDBTable(db_name, table_name, vars_list, flag='drop')

                downloadTableToDB(config.db_name, table_name, df)


# In[ ]:





# In[ ]:





# In[5]:


"""
Функция обращается в БД Медиаскоп и забирает данные из 2-х типов таблиц
для каждого типа медиа своя таблица, у них разный набор полей. но общая идея одинаковая
media_type - для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
mediascope_table - таблица Медиаскоп, из которой нужно получить данные tv_Ad / ArticleList2 и тд
mediascope_search_col_name - название поля Медиаскоп, по которому фильтруем строки в Медиаскоп
search_lst - список ИД, которые используются в условии для фильтрации (т.е. это именно те ИД, которые нам нужно добавить в наши справочники)
report - тип отчета 
base - выполняется для всех. просто добавляем media_key_id и другие кастомные ключи (если они есть)
simple - добавляем флаг чистки
buying - добавляем дисконт к расходам

"""

def get_adex_tables(mediascope_table, mediascope_fields_name, media_type='', int_lst=list(), float_lst=list(),
                      mediascope_search_col_name='', report='base', search_lst=''):
    search_lst_str = ''
    # если передан список уникальных ИД объявлений, то преобразуем список в строку
    if search_lst:      
        search_lst_str = config.get_lst_to_str(search_lst)
        # формируем условие для фильтрации запроса к БД
        search_lst_str = f'where {mediascope_search_col_name} in ({search_lst_str})'

    # отправляем запрос в БД Медиа инвестиции
    query = f'select {mediascope_fields_name} from {mediascope_table} {search_lst_str}' 
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)


    # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
    # Добавляем в датаФрейм - тип медиа, тип медиа+ИД объявления, расходы с дисконтом
    if media_type:
        df = append_custom_columns(df, report=report, media_type=media_type)

    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name)
    df = normalize_columns_types(df, int_lst, float_lst)
    return df


# In[ ]:





# In[6]:


"""
создаем, которая забирает из справочников объявлений Медиаскоп adId и их характеристики
только только для ИД, которые есть в нашей Базе
на вход принимаем тип  медиа
название таблицы-справочника в Медиаскоп, по которому нужно получить данные
Пример таблиц, которые здесь находятся
tv_Ad -> adex_ad_dict_list_tv
ra_Ad -> adex_ad_dict_list_radio
od_Ad -> adex_ad_dict_list_outdoor
pr_Ad -> adex_ad_dict_list_press
""" 

def update_media_ads_dict(media_type):
    # проверяем, если для этого типа медиа есть таблицы
    if media_type in config_media_costs.adex_ad_dicts:
        # забираем справочник с таблицами
        media_type_long = config.media_type_dict[media_type.lower()]
        tables_dict = config_media_costs.adex_ad_dicts[media_type]
        for key, value in tables_dict.items():
            # формируем переменные для запроса и подготовки данных
            table_name = tables_dict[key][0]
            vars_cols_lst = tables_dict[key][1]
            int_lst = tables_dict[key][2]
            mediascope_search_col_name = tables_dict[key][3]
            mssql_search_col_name = tables_dict[key][4]
            mediascope_fields_name = tables_dict[key][5]

            # формируем запрос к таблице Фактов
            query = f"select distinct {mssql_search_col_name} from media_{media_type_long}_costs"
            df_all_id = get_mssql_table(config.db_name, query=query)
            # формируем запрос к справочнику Объявлений, который будем обновлять
            query = f"select distinct {mssql_search_col_name} from {table_name}"
            df_prev_id = get_mssql_table(config.db_name, query=query)
            # оставляем ИД, которые есть в таблице фактов, но их нет в справочнике
            search_lst = list(set(df_all_id[mssql_search_col_name]) - set(df_prev_id[mssql_search_col_name]))

            # если существуют новые ИД, то отправляем запрос в Медиаскоп, чтобы получить нужные ИД со всеми характеристиками
            if search_lst:
                df = get_adex_tables(mediascope_table=key, 
                                     mediascope_fields_name=mediascope_fields_name, 
                                     media_type=media_type, 
                                     int_lst=int_lst,
                                      mediascope_search_col_name=mediascope_search_col_name, 
                                     report='ad', 
                                     search_lst=search_lst)

                del df_all_id, df_prev_id

                # приводим дату первого показа к нужному формату
                if 'fiss' in list(df.columns):
                    df['fiss'] = df['fiss'].astype('str')
                    df['adFirstIssueDate'] = df['fiss'].apply(lambda x: x[:4] + '-' + x[4:6] + '-' + x[6:] if x!='' else x)

                # убираем не нужный текст после пробела, например brand_main nvarchar(150) -> brand_main
                cols_lst = [i[:i.find(' ')] for i in vars_cols_lst]
                # оставляем в датаФрейме только нужные поля
                df = df[cols_lst]
                # приводим типы данных в порядок
                df = normalize_columns_types(df, int_lst)
                # createDBTable(config.db_name, table_name, vars_cols_lst, flag='drop')
                downloadTableToDB(config.db_name, table_name, df)
            else:
                print(sep_str)
                print(f'Новых ИД НЕТ для загрузки в таблицу {table_name}')
                print(sep_str)


# In[ ]:





# In[7]:


"""
Функция забирает и обновляет справочники, которые используются только в одном Медиа и никаком другом
например - tv_Ad_slogan_audio / tv_Ad_slogan_video
"""


def update_another_dicts(media_type):
    # проверяем, если для этого типа медиа есть таблицы
    if media_type in config_media_costs.adex_unique_media_dicts:
        # забираем справочник с таблицами
        media_type_long = config.media_type_dict[media_type.lower()]
        tables_dict = config_media_costs.adex_unique_media_dicts[media_type]
        # формируем переменные для запроса и подготовки данных
        for key, value in tables_dict.items():
            table_name = tables_dict[key][0]
            vars_cols_lst = tables_dict[key][1]
            int_lst = tables_dict[key][2]
            mediascope_search_col_name = tables_dict[key][3]
            mssql_search_col_name = tables_dict[key][4]
            mediascope_fields_name = tables_dict[key][5]

            # формируем запрос к справочнику объявлений, из которого нам нужно достать характеристику
            query = f"select distinct {mssql_search_col_name} from adex_ad_dict_list_{media_type_long}"
            df_all_id = get_mssql_table(config.db_name, query=query)
            # формируем запрос к справочнику Объявлений, который будем обновлять
            query = f"select distinct {mssql_search_col_name} from {table_name}"
            df_prev_id = get_mssql_table(config.db_name, query=query)
            # если существуют новые ИД, то отправляем запрос в Медиаскоп, чтобы получить нужные ИД со всеми характеристиками
            search_lst = list(set(df_all_id[mssql_search_col_name]) - set(df_prev_id[mssql_search_col_name]))


            if search_lst:
                df = get_adex_tables(mediascope_table=key, 
                                     mediascope_fields_name=mediascope_fields_name, 
                                     media_type=media_type, 
                                     int_lst=int_lst,
                                      mediascope_search_col_name=mediascope_search_col_name, 
                                     report='base', 
                                     search_lst=search_lst)

                del df_all_id, df_prev_id

                # убираем не нужный текст после пробела, например brand_main nvarchar(150) -> brand_main
                cols_lst = [i[:i.find(' ')] for i in vars_cols_lst]
                # оставляем в датаФрейме только нужные поля
                df = df[cols_lst]
                # приводим типы данных в порядок
                df = normalize_columns_types(df, int_lst)
                # createDBTable(config.db_name, table_name, vars_cols_lst, flag='drop')
                downloadTableToDB(config.db_name, table_name, df)

            else:
                print(sep_str)
                print(f'Новых ИД НЕТ для загрузки в таблицу {table_name}')
                print(sep_str)


# In[ ]:





# In[8]:


"""
Функция для обновления общих справочников для ВСЕХ медиа
Пример таблиц
AdvertiserList / BrandList / Subbrandlist / ModelList / ArticleList2 / ArticleList3 / ArticleList4
"""

def update_list_dicts(media_type):
    # забираем справочник с таблицами
    media_type_long = config.media_type_dict[media_type.lower()]
    tables_dict = config_media_costs.adex_all_media_list_dicts
    # формируем переменные для запроса и подготовки данных
    for key, value in tables_dict.items():
        table_name = tables_dict[key][0]
        vars_cols_lst = tables_dict[key][1]
        int_lst = tables_dict[key][2]
        mediascope_search_col_name = tables_dict[key][3]
        mssql_search_col_name = tables_dict[key][4]
        mediascope_fields_name = tables_dict[key][5]

        # формируем запрос к справочнику объявлений, из которого нам нужно достать характеристику
        query = f"select distinct {mssql_search_col_name} from adex_ad_dict_list_{media_type_long}"
        df_all_id = get_mssql_table(config.db_name, query=query)
        # формируем запрос к справочнику Объявлений, который будем обновлять
        query = f"select distinct {mssql_search_col_name} from {table_name}"
        df_prev_id = get_mssql_table(config.db_name, query=query)
        # если существуют новые ИД, то отправляем запрос в Медиаскоп, чтобы получить нужные ИД со всеми характеристиками
        search_lst = list(set(df_all_id[mssql_search_col_name]) - set(df_prev_id[mssql_search_col_name]))

        if search_lst:
            df = get_adex_tables(mediascope_table=key, 
                                 mediascope_fields_name=mediascope_fields_name, 
                                 media_type=media_type, 
                                 int_lst=int_lst,
                                  mediascope_search_col_name=mediascope_search_col_name, 
                                 report='base', 
                                 search_lst=search_lst)

            del df_all_id, df_prev_id


            # убираем не нужный текст после пробела, например brand_main nvarchar(150) -> brand_main
            cols_lst = [i[:i.find(' ')] for i in vars_cols_lst]
            # оставляем в датаФрейме только нужные поля
            df = df[cols_lst]
            # приводим типы данных в порядок
            df = normalize_columns_types(df, int_lst)            
            # createDBTable(config.db_name, table_name, vars_cols_lst, flag='drop')
            downloadTableToDB(config.db_name, table_name, df)

        else:
            print(sep_str)
            print(f'Новых ИД НЕТ для загрузки в таблицу {table_name}')
            print(sep_str)


# In[9]:


"""
Функция обновляет справочники по каждому типу Медиа
В данном случае, если в таблице чистки изменились значения Флага, Категории и тд
Мы перезаписываем справочник с новыми кастомными значениями
"""
def update_ad_dict_items(media_type):
    # забирем для каждого типа медиа, переменные для справочника объявлений
    if media_type in config_media_costs.adex_ad_dicts:
        # забираем справочник с таблицами
        media_type_long = config.media_type_dict[media_type.lower()]
        tables_dict = config_media_costs.adex_ad_dicts[media_type]

        for key, value in tables_dict.items():
            # формируем переменные для запроса и подготовки данных
            table_name = tables_dict[key][0]
            vars_cols_lst = tables_dict[key][1]
            int_lst = tables_dict[key][2]

            # формируем запрос к справочнику объявлений, из которого нам нужно достать характеристику
            query = f"""select * from adex_ad_dict_list_{media_type_long}"""
            df = get_mssql_table(config.db_name, query=query)

            # оставляем поля, которых НЕТ в гугл докс чистке
            base_cols = [i[:i.find(' ')] for i in config.custom_ad_dict_vars_list]
            base_cols = list(set(df.columns) - set(base_cols))
            df = df[base_cols]

            # добавляем поля из гугл докс Чистки, в которых могли быть изменения (категория, флаг чистки и тд)
            df = append_custom_columns(df, report='ad', media_type=media_type)
            df = normalize_columns_types(df, int_lst)
            # оставляем поля, которые должны быть в итоговой таблице Объявлений для конкретного Медиа
            base_cols = [i[:i.find(' ')] for i in vars_cols_lst]
            df = df[base_cols]
            # удаляем справочник из БД и заливаем с новыми значениями
            createDBTable(config.db_name, table_name, vars_cols_lst, flag='drop')
            downloadTableToDB(config.db_name, table_name, df)


# In[10]:


"""
Функция для создания Представлений
объединяем общие таблицы справочники в одно представление
Запускается 1 раз при первой загрузке
"""
def create_adex_views():
    for key, value in config_media_costs.media_view_dicts.items():
        createView(config.db_name, table_name=key, cond=value)


# In[ ]:





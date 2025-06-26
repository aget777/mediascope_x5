#!/usr/bin/env python
# coding: utf-8

# In[3]:


# добавляем библиотеки для работы с ТВ индексом
get_ipython().run_line_magic('reload_ext', 'autoreload')
get_ipython().run_line_magic('autoreload', '2')
import asyncio
import os
import gc
import sys
import re
import json
from datetime import datetime, date, timedelta, time
import time
from dateutil.relativedelta import relativedelta
import warnings
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

import config
# import config_tv_index
import config_reg_tv
# from config_search_funcs import get_subbrand_id_str
from normalize_funcs import normalize_columns_types, append_custom_columns, get_tv_type_ooh_reg, get_cleaning_dict, get_media_discounts
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table, removeRowsFromDB
from create_dicts_tv_index import download_tv_index_default_dicts


# Cоздаем объекты для работы с TVI API
mnet = mscore.MediascopeApiNetwork()
mtask = cwt.MediaVortexTask()
cats = cwc.MediaVortexCats()

# Забриае название БД
db_name = config.db_name

# subbrand_id_str = get_subbrand_id_str(mon_num='360', media_type='tv')
# special_ad_filter = f'{config_tv_index.nat_tv_ad_filter} AND subbrandId IN ({subbrand_id_str})'
# nat_tv_ad_dict = config.nat_tv_ad_dict


# In[4]:


# Включаем отображение всех колонок
pd.set_option('display.max_columns', None)
# Задаем ширину столбцов по контенту
pd.set_option('display.max_colwidth', None)
pd.set_option('display.max_rows', None)

warnings.simplefilter(action='ignore', category=FutureWarning)
# убираем лишние предупреждения
pd.set_option('mode.chained_assignment', None)

sep_str = '*' * 50


# In[5]:


def create_reg_tv_tables():
    # специально не стал делать цикл, чтобы явно показать, какие именно создаем таблицы
    createDBTable(config.db_name, config_reg_tv.reg_tv_simple , config_reg_tv.reg_tv_simple_vars_list, flag='create')
    createDBTable(config.db_name, config_reg_tv.reg_tv_buying , config_reg_tv.reg_tv_buying_vars_list, flag='create')
    # создаем пустую таблицу-справочник объявлений
    createDBTable(config.db_name, config_reg_tv.reg_tv_ad_dict , config_reg_tv.reg_tv_ad_dict_vars_list, flag='drop')


# In[ ]:


"""
основная функция для получения данных из Медиаскоп Рег ТВ
отчеты Simple / Buying
если это первая загрузка, то создаем нужные таблицы в БД
добавляем флаг Чистки из гугл докса / если объявление НЕ встречалось, ставим флаг 2
номализуем данные - приводим к верхнему регистру / округляем до 2-х знаков после запятой
"""

def get_reg_tv_reports(ad_filter=False, start_date='', end_date='', flag='regular'):
    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}')
    # Здесь указаны логические условия ad_filter 
    # Они применяются для получения статистики в отчетах Simple и Buying# условия фильтрации для запроса 
    if not ad_filter:
        ad_filter = config_reg_tv.reg_tv_ad_filter

    if flag.lower()=='first':
        create_reg_tv_tables()

    # получаем данные из гугл диска с чисткой объявлений 
    # - передаем список Типов медиа, чтобы оставить нужные значения
    # custom_cleaning_dict = get_cleaning_dict(media_type_lst)
    # список полей из гугл таблицы Чистка, которые нужно добавить в справочник объявлений
    custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
    # убираем некоторые поля, т.е. их заберем из отчета Simple
    custom_cols_list = list(set(custom_cols_list) - set(['media_type', 'media_type_long', 'include_exclude', 'tv_type_ooh_reg']))
    custom_cols_str = ', '.join(custom_cols_list)

    print(sep_str)
    print('Забираем из БД НЕ ОБНОВЛЕННЫЙ справочник объявлений')

    query = f"select {custom_cols_str}  from {config_reg_tv.reg_tv_ad_dict}"
    reg_tv_ad_dict_df = get_mssql_table(config.db_name, query=query)
    print(sep_str)

    # # Итоговый список полей, который нам нужен в словаре объявлений
    reg_tv_ad_dict_cols_list = [col[:col.find(' ')] for col in config_reg_tv.reg_tv_ad_dict_vars_list]

# Если start_date НЕ передается в функцию - мы считаем, что это еженедельное обновление
# Данные в БД TV Index идут с отставанием 3 дня от текущей даты
# Нам необходимо перезаписать последние 14 дней
# Из текуще даты мы вычитаем 3 дня задержки и 14 дней для перезаписи
# это будет считаться датой начала загрузки
# удалаем в БД все строки ничиная с этой даты

    if not start_date:
        start_date = (datetime.now().date()  - timedelta(days=14+3))

    # Если дата окончани загрузки НЕ задана, то мы считаем минус 3 дня от текущей даты
    if not end_date:
        end_date = (datetime.now().date()  - timedelta(days=3))

    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').date()
    end_date = datetime.strptime(str(end_date), '%Y-%m-%d').date()

    # если это НЕ первая загрузка, то удаляем строки из БД начиная с даты начала текущей загрузки
    # для отчетов Buying - расходы приходят с запозданием на неделю, а так же страхуемся от возможных дублей в БД при новой загрузке
    # Например если сегодня 30-е число месяца
    # БД еще не обновилась, значит у нас даты до 23-го числа месяца
    # Причем с 16 по 23 Расходов НЕТ
    # мы запускаем обновление, т.е. забираем данные с 24 по 30
    # При этом нам нужно обновить Расхходы с 16 по 23

    if flag=='regular':
        cond = f"researchDate >= '{str(start_date)}' and researchDate <= '{str(end_date)}'"

        print()
        print(sep_str)
        print(f'Удалем строки из таблицы: reg_tv_simple и reg_tv_buying по условию: {cond}')
        print()

        removeRowsFromDB(config.db_name, config_reg_tv.reg_tv_simple, cond)
        removeRowsFromDB(config.db_name, config_reg_tv.reg_tv_buying, cond)
        print()
    # считаем кол-во дней в периоде
    # каждый день мы будем забирать по отдельности и записывать его в БД
    count_days = (end_date - start_date).days

    print()
    print(f'Загружаем отчет за период {start_date} - {end_date}. Общее количество дней: {count_days+1}')
    print(sep_str)
    print()

    # проходимся по общему количеству дней
    for i in range(count_days+1):
        # формируем отдельную дату для загрузки
        cur_date = str(start_date + relativedelta(days=i))
        date_filter = [(cur_date, cur_date)]
         # Посчитаем задания в цикле
        tasks = []
        print()
        print(f'{"="*10}Загружаем {cur_date}. Отправляем задания на расчет')
        print()


        for num, (reg_id, reg_name) in enumerate(config_reg_tv.regions_dict.items()):
            company_filter = f'regionId IN ({reg_id})'

            print(f'{"-"*20} Забираем статистику по отчету Buying -- Region_name: {reg_name} --Дата  {cur_date}')
            print(f'Порядковый номер региона {num} из {len(config_reg_tv.regions_dict)}')
            print()
            # Забираем статистику по отчету Buying
            df_buying_final = get_reg_tv_buying_report(ad_filter=ad_filter, date_filter=date_filter, company_filter=company_filter)
            df_buying_final['prj_name'] = 'Total_buying_aud'
            # Добавляем тип медиа и Дисконты к расходым по годам
            df_buying_final = append_custom_columns(df_buying_final, report='buying')
            # Прописываем условия для создания колонки tv_type_ooh_reg
            df_buying_final = get_tv_type_ooh_reg(df_buying_final)

            # забираем список полей, которые созданы в БД в таблице фактов Simple
            reg_tv_buying_cols_lst = [col[:col.find(' ')] for col in config_reg_tv.reg_tv_buying_vars_list]
            # оставляем только нужные поля в датаФрейме для загрузки в БД
            df_buying_final = df_buying_final[reg_tv_buying_cols_lst]
            # нормализуем отчет по Баинговой аудитории
            df_buying_final = normalize_columns_types(df_buying_final, config_reg_tv.reg_tv_buying_int_lst, config_reg_tv.reg_tv_buying_float_lst)


            # загружаем данные в БД по отчету Buying
            print('Записываем данные в таблицу фактов Buying')
            print()
            # записываем в БД отчет по Баинговой аудитории
            downloadTableToDB(config.db_name, config_reg_tv.reg_tv_buying, df_buying_final)
            # удаляем этот датаФрейм
            del df_buying_final

            print()
            print(sep_str)

            print()
            print(f'{"-"*20} Забираем статистику по отчету Simple -- Region_name: {reg_name} --Дата:  {cur_date}')
            print(f'Порядковый номер региона {num} из {len(config_reg_tv.regions_dict)}')
            print()
            # Забираем статистику по отчету Simple
            df_simple_final = get_reg_tv_simple_report(ad_filter=ad_filter, date_filter=date_filter, company_filter=company_filter)
            # добавляем флаг чистки в датаФрейм
            df_simple_final = append_custom_columns(df_simple_final, report='simple', nat_tv_ad_dict=reg_tv_ad_dict_df)
            # Прописываем условия для создания колонки tv_type_ooh_reg
            df_simple_final = get_tv_type_ooh_reg(df_simple_final)
            # Нормализуем все поля в датаФрейме
            df_simple_final = normalize_columns_types(df_simple_final, config_reg_tv.reg_tv_simple_int_lst, config_reg_tv.reg_tv_simple_float_lst)

            # создаем таблицу с уникальными объявлениями и их характеристиками
            simple_ad_dict_df = df_simple_final[reg_tv_ad_dict_cols_list].drop_duplicates('media_key_id')

            # забираем из БД из справочника объявлений уникальные ИД
            query = f"select distinct adId  from {config_reg_tv.reg_tv_ad_dict}"
            reg_tv_ad_id_dict = get_mssql_table(config.db_name, query=query)
            # создаем список Уникальных ИД Объявлений, которые уже есть в справочнике в БД
            reg_tv_ad_id_lst = list(reg_tv_ad_id_dict['adId'])
            # оставляем только те объявления, которых нет в справочнике
            simple_ad_dict_df = simple_ad_dict_df.query('adId not in @reg_tv_ad_id_lst')


            # на всякий случай обрезаем описание объявления до 500 символов, чтобы не было переполнения строки
            simple_ad_dict_df['adNotes'] = simple_ad_dict_df['adNotes'].str.slice(0, 500)
            # нормализуем типы данных
            simple_ad_dict_df = normalize_columns_types(simple_ad_dict_df, config_reg_tv.reg_tv_ad_dict_int_lst)

            print()
            print(sep_str)
            print('Записываем данные в справочник объявлений REG TV Index')
            print()

            downloadTableToDB(config.db_name, config_reg_tv.reg_tv_ad_dict, simple_ad_dict_df)

            print(sep_str)
            print()

            # забираем список полей, которые созданы в БД в таблице фактов Simple
            reg_tv_simple_cols_lst = [col[:col.find(' ')] for col in config_reg_tv.reg_tv_simple_vars_list]
            # оставляем только нужные поля в датаФрейме для загрузки в БД
            df_simple_final = df_simple_final[reg_tv_simple_cols_lst]

            # загружаем данные в БД по отчету Simple
            print('Записываем данные в таблицу фактов Simple')
            print()
            downloadTableToDB(config.db_name, config_reg_tv.reg_tv_simple, df_simple_final)

            print(sep_str)
            print()

    finish_time = datetime.now()
    print(sep_str)
    print(f'Время окончания работы скрипта {finish_time}')
    print(f'Период загрузки {start_date} - {end_date}. Общее время работы скрипта: {finish_time - start_time}')
    print(sep_str)


# In[ ]:





# In[ ]:


"""
УКАЗЫВАЕМ на листе config
targets - список аудиторий, по которым нужно собрать датаФрейм
ad_filter - условия фильтрации объявлений 


weekday_filter - Задаем дни недели / Задаем тип дня - daytype_filter / Задаем временной интервал - time_filter
Задаем ЦА - basedemo_filter / 
Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index - targetdemo_filter
Задаем место просмотра - location_filter / Задаем каналы - company_filter
Указываем фильтр программ: продолжительность от 5 минут (300 секунд) - program_filter / Фильтр блоков - 
"""

def get_reg_tv_simple_report(ad_filter=None, weekday_filter=None, date_filter=None,time_filter=None, basedemo_filter=None, targetdemo_filter=None, 
                             daytype_filter=None,location_filter=None, company_filter=None, program_filter=None, break_filter=None, sortings=None,
                            add_city_to_basedemo_from_region=True, add_city_to_targetdemo_from_region=True):
    # Cоздаем объекты для работы с TVI API
    mnet = mscore.MediascopeApiNetwork()
    mtask = cwt.MediaVortexTask()
    cats = cwc.MediaVortexCats()
    tasks = []
    # список аудиторий, по которым собираем статистику
    targets = config_reg_tv.reg_tv_targets
    # список срезов, по которым будет разбивка отчета
    slices = config_reg_tv.reg_tv_slices
    # список метрик для отчета Simple
    statistics = config_reg_tv.reg_tv_simple_statistics
    # Опции для расчета - вся рф и тд.
    options = config_reg_tv.reg_tv_options

    if targets:
        # Для каждой ЦА формируем задание и отправляем на расчет
        for target, syntax in targets.items():
            # Подставляем значения словаря в параметры
            project_name = f"{target}" 
            basedemo_filter = f"{syntax}"

            # Формируем задание для API TV Index в формате JSON
            task_json = mtask.build_simple_task(date_filter=date_filter, weekday_filter=weekday_filter, 
                                                daytype_filter=daytype_filter, company_filter=company_filter, 
                                                location_filter=location_filter, basedemo_filter=basedemo_filter, 
                                                targetdemo_filter=targetdemo_filter,program_filter=program_filter, 
                                                break_filter=break_filter, ad_filter=ad_filter, 
                                                slices=slices, statistics=statistics, sortings=sortings, options=options,
                                               add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                                                add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)

            # Для каждого этапа цикла формируем словарь с параметрами и отправленным заданием на расчет
            tsk = {}
            tsk['project_name'] = project_name    
            tsk['task'] = mtask.send_simple_task(task_json)
            tasks.append(tsk)
            time.sleep(2)

        print('')
        # Ждем выполнения
        print('Ждем выполнения')
        tsks = mtask.wait_task(tasks)
        print('Расчет завершен, получаем результат')

        # Получаем результат
        results = []
        print('Собираем таблицу')

        for t in tasks:
            tsk = t['task'] 
            df_result = mtask.result2table(mtask.get_result(tsk), project_name = t['project_name'])        
            results.append(df_result)
            print('.', end = '')

        df = pd.concat(results)
        df = df[['prj_name']+slices+statistics]

        return df


# In[ ]:





# In[ ]:


"""
УКАЗЫВАЕМ на листе config
targets - здесь Обнуляем, т.к. для нам нужен Тотал
ad_filter - условия фильтрации объявлений 


weekday_filter - Задаем дни недели / Задаем тип дня - daytype_filter / Задаем временной интервал - time_filter
Задаем ЦА - basedemo_filter / 
Доп фильтр ЦА, нужен только в случае расчета отношения между ЦА, например, при расчете Affinity Index - targetdemo_filter
Задаем место просмотра - location_filter / Задаем каналы - company_filter
Указываем фильтр программ: продолжительность от 5 минут (300 секунд) - program_filter / Фильтр блоков - 
"""

def get_reg_tv_buying_report(ad_filter=None, weekday_filter=None, date_filter=None,time_filter=None, basedemo_filter=None, targetdemo_filter=None, 
                             daytype_filter=None,location_filter=None, company_filter=None, program_filter=None, break_filter=None, sortings=None,
                            add_city_to_basedemo_from_region=True, add_city_to_targetdemo_from_region=True):
    # Cоздаем объекты для работы с TVI API
    mnet = mscore.MediascopeApiNetwork()
    mtask = cwt.MediaVortexTask()
    cats = cwc.MediaVortexCats()
    tasks = []

    # список срезов, по которым будет разбивка отчета по Баинговым аудиториям
    slices = config_reg_tv.reg_tv_buying_slices
    # список метрик для отчета Buying
    statistics = config_reg_tv.reg_tv_bying_statistics
    # Опции для расчета - вся рф и тд.
    options = config_reg_tv.reg_tv_options

    # Формируем задание для API TV Index в формате JSON
    task_json = mtask.build_simple_task(date_filter=date_filter, weekday_filter=weekday_filter, 
                                        daytype_filter=daytype_filter, company_filter=company_filter, 
                                        location_filter=location_filter, basedemo_filter=basedemo_filter, 
                                        targetdemo_filter=targetdemo_filter,program_filter=program_filter, 
                                        break_filter=break_filter, ad_filter=ad_filter, 
                                        slices=slices, statistics=statistics, sortings=sortings, options=options,
                                       add_city_to_basedemo_from_region=add_city_to_basedemo_from_region,
                                        add_city_to_targetdemo_from_region=add_city_to_targetdemo_from_region)


    # Отправляем задание на расчет и ждем выполнения
    task_timeband = mtask.wait_task(mtask.send_simple_task(task_json))
    # Получаем результат
    df = mtask.result2table(mtask.get_result(task_timeband))

    return df


# In[ ]:


# config_reg_tv.reg_tv_options


# In[ ]:





# In[ ]:


# start_date = '2024-01-01'
# end_date = '2024-01-01'

# date_filter = [(start_date, end_date)]
# ad_filter = config_reg_tv.reg_tv_ad_filter


# In[ ]:


# regions_dict = {
#     40: 'БАРНАУЛ',
#     # 18: 'ВЛАДИВОСТОК',
#     # 5: 'ВОЛГОГРАД',
#     # 8: 'ВОРОНЕЖ',
#     # 12: 'ЕКАТЕРИНБУРГ',
#     # 25: 'ИРКУТСК',
#     # 19: 'КАЗАНЬ',
#     # 45: 'КЕМЕРОВО',
#     # 23: 'КРАСНОДАР',
#     # 17: 'КРАСНОЯРСК',
#     # 1: 'МОСКВА',
#     # 4: 'НИЖНИЙ НОВГОРОД',
#     # 15: 'НОВОСИБИРСК',
#     # 21: 'ОМСК',
#     # 14: 'ПЕРМЬ',
#     # 9: 'РОСТОВ-НА-ДОНУ',
#     # 6: 'САМАРА',
#     # 2: 'САНКТ-ПЕТЕРБУРГ',
#     # 10: 'САРАТОВ',
#     # 39: 'СТАВРОПОЛЬ',
#     # 3: 'ТВЕРЬ',
#     # 55: 'ТОМСК',
#     # 16: 'ТЮМЕНЬ',
#     # 20: 'УФА',
#     # 26: 'ХАБАРОВСК',
#     # 13: 'ЧЕЛЯБИНСК',
#     # 7: 'ЯРОСЛАВЛЬ'
# }


# In[ ]:


# df_buying_final = pd.DataFrame()
# for reg_id, reg_name in regions_dict.items():
#     company_filter = f'regionId IN ({reg_id})'
#     reg_df = get_reg_tv_buying_report(ad_filter=ad_filter, date_filter=date_filter, company_filter=company_filter)
#     df_buying_final = pd.concat([df_buying_final, reg_df])


# In[ ]:





# In[ ]:


# df_buying_final['ConsolidatedCostRUB'].sum()


# In[ ]:


# df_buying_final.head(1)


# In[ ]:


# custom_cols_list = [col[:col.find(' ')] for col in config.custom_ad_dict_vars_list]
# # убираем некоторые поля, т.е. их заберем из отчета Simple
# custom_cols_list = list(set(custom_cols_list) - set(['media_type', 'media_type_long', 'include_exclude', 'tv_type_ooh_reg']))
# custom_cols_str = ', '.join(custom_cols_list)

# print(sep_str)
# print('Забираем из БД НЕ ОБНОВЛЕННЫЙ справочник объявлений')

# query = f"select {custom_cols_str}  from {config_reg_tv.reg_tv_ad_dict}"
# reg_tv_ad_dict_df = get_mssql_table(config.db_name, query=query)
# print(sep_str)

# # # Итоговый список полей, который нам нужен в словаре объявлений
# reg_tv_ad_dict_cols_list = [col[:col.find(' ')] for col in config_reg_tv.reg_tv_ad_dict_vars_list]


# In[ ]:


# df_simple_final = append_custom_columns(df_simple_final, report='simple', nat_tv_ad_dict=reg_tv_ad_dict_df)
# # Прописываем условия для создания колонки tv_type_ooh_reg
# df_simple_final = get_tv_type_ooh_reg(df_simple_final)
# # Нормализуем все поля в датаФрейме
# df_simple_final = normalize_columns_types(df_simple_final, config_reg_tv.reg_tv_simple_int_lst, config_reg_tv.reg_tv_simple_float_lst)

# # создаем таблицу с уникальными объявлениями и их характеристиками
# simple_ad_dict_df = df_simple_final[reg_tv_ad_dict_cols_list].drop_duplicates('media_key_id')


# In[ ]:


# # забираем список полей, которые созданы в БД в таблице фактов Simple
# reg_tv_simple_cols_lst = [col[:col.find(' ')] for col in config_reg_tv.reg_tv_simple_vars_list]
# # оставляем только нужные поля в датаФрейме для загрузки в БД
# df_simple_final = df_simple_final[reg_tv_simple_cols_lst]


# In[ ]:


# df_buying_final = pd.DataFrame()
# for reg_id, reg_name in regions_dict.items():
#     company_filter = f'regionId IN ({reg_id})'
#     reg_df = get_reg_tv_buying_report(ad_filter=ad_filter, date_filter=date_filter, company_filter=company_filter)
#     df_buying_final = pd.concat([df_buying_final, reg_df])


# In[ ]:


# createDBTable(config.db_name, config_reg_tv.reg_tv_simple , config_reg_tv.reg_tv_simple_vars_list, flag='create')


# In[ ]:





# In[ ]:


# downloadTableToDB(config.db_name, config_reg_tv.reg_tv_simple, df_simple_final)


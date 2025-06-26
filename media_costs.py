#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import pandas as pd
import numpy as np
from math import ceil
import os
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import time
import pyarrow as pa
from urllib.parse import quote_plus
from pandas.api.types import is_string_dtype
import gc
import warnings

import config
import config_media_costs
from config_search_funcs import get_subbrand_id_str, get_outdoor_regions
from normalize_funcs import normalize_columns_types, append_custom_columns, get_cleaning_dict, get_media_discounts, get_tv_type_ooh_reg
from db_funcs import createDBTable, downloadTableToDB, get_mssql_table, removeRowsFromDB, get_mssql_russian_chars



# regions_id = get_outdoor_regions()
# from create_dicts_adex import get_adex_ad_dicts, download_adex_default_media_type_dicts

# start_date = '2025-02-01'#'2023-01-01'
# start_date = datetime.strptime(start_date, '%Y-%m-%d').date()

# end_date = '2025-02-02'
# end_date = datetime.strptime(end_date, '%Y-%m-%d').date()

# print(f'start_date: {start_date} / end_date: {end_date}')


# In[ ]:





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


"""
создаем функцию, которая вернет номер месяца от начальной даты отсчета
"""

def get_mon_num_from_date(curr_date):
    curr_date = datetime.strptime(str(curr_date), '%Y-%m-%d')
    # с помощью метода relativedelta получаем кол-во лет и месяцев от даты начала до текущей даты
    relative_date = relativedelta(curr_date, config_media_costs.start_of_the_time)
    # теперь нужно получить номер текущего месяца от даты начала
    months_count = relative_date.years * 12 + relative_date.months

    return months_count


# In[ ]:


# get_mon_num_from_date('2025-01-01')


# In[ ]:


# mon_num = 423

# query = f"""select distinct mon from tv_Ad_month where mon={str(mon_num)}"""
# # отправляем запрос в БД Медиа инвестиции
# check_data = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)

# if check_data.empty:
#     print("Данных за этот месяц нет")


# In[ ]:


# check_data.empty


# In[ ]:


def get_media_costs_report(start_date='', end_date='', media_type='tv', flag='regular', main_filter=False):

    start_time = datetime.now()
    print(f'Скрипт запущен {start_time}')

    if not main_filter:
        main_filter=config_media_costs.main_filter_str


    cur_date = datetime.now().date()
    cur_year_month = cur_date.strftime('%Y-%m')

# если дата начала задана, то приводим ее к формату Даты
# иначе оставляем пусто
    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').date() if start_date else ''
# забираем год-месяц для проверки относительно текущего месяца
    start_year_month = start_date.strftime('%Y-%m') if start_date else ''

# если дата окончания задана, то приводим ее к формату Даты
# иначе оставляем пусто
    end_date = datetime.strptime(str(end_date), '%Y-%m-%d').date() if end_date else ''
# забираем год-месяц для проверки относительно текущего месяца
    end_year_month = end_date.strftime('%Y-%m') if end_date else ''

# если дата начала НЕ задана, значит - это ежемесячное обновление
# от текущей даты берем 2 месяца назад
# удаляем из БД эти месяцы
# далее загрузим новые данные - до Текущий месяц минус один (в Медиаское запоздание на 1 месяц)

    if not start_date or start_year_month==cur_year_month:
        start_date = (cur_date  - relativedelta(months=2))

# если даты окончания нет или она равна текущему месяцу, то задаем прошлый месяц
    if not end_date or end_year_month==cur_year_month:
        end_date = (cur_date  - relativedelta(months=1))

# получаем номер месяца начала/окончания загрузки данных по летоисчислению Медиаскоп
    start_mon = get_mon_num_from_date(start_date)
    end_mon = get_mon_num_from_date(end_date)

    # если это НЕ первая загрузка, то удаляем строки из БД начиная с даты начала текущей загрузки
    # для отчетов Buying - расходы приходят с запозданием на неделю, а так же страхуемся от возможных дублей в БД при новой загрузке
    if flag=='regular':
        # Нам необходимо защититься от возможного дублирования строк при регулярном обновлении данных
        # для этого мы на всякий случай пробуем удалить из БД строки за запрашиваемый период
        # НО в Медиаскопе может не быть новых в данных
        # в этом случае получится, что мы удалили из нашей БД строки, а в замен ничего не получили
        # поэтому добавляем проверку - отправляем запрос в Медиаскоп, чтобы проверить, есть ли там МАКСИМАЛЬНЫЙ месяц из запроса
        # если нужного месяца нет, то прерываем выполенение кода
        query = f"""select distinct mon from tv_Ad_month where mon={str(end_mon)}"""
        # отправляем запрос в БД Медиа инвестиции
        check_data = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
        if check_data.empty:
            print(sep_str)
            print(f'Данных за Максимальный месяц в запросе нет - {end_date}')
            print(sep_str)

            return

        cond = f'mon_num >= {start_mon} and mon_num <= {end_mon}'

        print()
        print(sep_str)
        print(f'Удалем строки из таблицы: media_{config.media_type_dict[media_type]}_costs по условию: {cond}')
        print()

        removeRowsFromDB(config.db_name, f'media_{config.media_type_dict[media_type]}_costs', cond)
        print()

    # считаем кол-во месяцев в периоде
    # каждый месяц мы будем забирать по отдельности и записывать его в БД
    total_years = relativedelta(end_date, start_date).years * 12
    count_months = relativedelta(end_date, start_date).months + total_years + 1


    print()
    print(f'Загружаем отчет за период {start_date} - {end_date}. Общее количество месяцев: {count_months}')
    print(sep_str)
    print()

    # проходимся по общему количеству дней
    for i in range(count_months):
        # формируем отдельную дату для загрузки
        cur_date = start_date + relativedelta(months=i)
        cur_mon = get_mon_num_from_date(cur_date)
        print()
        print(f'{"="*10}Загружаем {cur_date}. Статистика по {media_type}')
        print()

        table_name = config_media_costs.media_dicts_costs[media_type][0]
        vars_cols_lst = config_media_costs.media_dicts_costs[media_type][1]
        int_lst = config_media_costs.media_dicts_costs[media_type][2]
        float_lst = config_media_costs.media_dicts_costs[media_type][3]
# Забираем статистику по ТВ
        if media_type=='tv':
            df = get_table_tv_costs(int_lst, 
                               float_lst,
                               vars_cols_lst,
                               start_date=str(cur_date), 
                               mon_num=int(cur_mon), 
                               media_type=media_type,
                                main_filter=main_filter)
# Забираем статистику по Радио
        if media_type=='ra':
            df = get_table_radio_costs(int_lst, 
                               float_lst, 
                               vars_cols_lst,
                               start_date=str(cur_date), 
                               mon_num=int(cur_mon), 
                               media_type=media_type,
                                main_filter=main_filter)
# Забираем статистику по Наружке
        if media_type=='od':
            df = get_table_outdoor_costs(int_lst, 
                               float_lst, 
                               vars_cols_lst,
                               start_date=str(cur_date), 
                               mon_num=int(cur_mon), 
                               media_type=media_type,
                                main_filter=main_filter)
# Забираем статистику по Прессе
        if media_type=='pr':
            df = get_table_press_costs(int_lst, 
                               float_lst, 
                               vars_cols_lst,
                               start_date=str(cur_date), 
                               mon_num=int(cur_mon), 
                               media_type=media_type,
                                main_filter=main_filter)

        downloadTableToDB(config.db_name, table_name, df)



# In[ ]:


# start_date='2023-01-01'
# end_date='2023-02-01'
# for media_type in config_media_costs.media_type_lst:
#     get_media_costs_report(start_date=start_date, end_date=end_date, media_type=media_type, flag='first')


# In[ ]:


"""
ТВ расходы - создаем функцию, которая 
- забирает расходы по месяцам
- переименовывает поля (приводит к стандарту ТВ Индекс
Функция принимает на вход
*normalize_lst - передаем, как отдельные параметры - Список полей с типом int / Список полей с типом float
start_mon - дата начала месяца, за который забираем статистику
mon_num - номер месяца по летоисчислению Медиаскоп
media_type - тип медиа, по которому забираем расходы
"""

def get_table_tv_costs(*normalize_lst, media_type, start_date='2023-01-01', mon_num=396, main_filter=None):
    # для запроса к БД приводим тип медиа к нижнему регистру
    media_type = media_type.lower()
    media_type_long = config.media_type_dict[media_type]
    # Формируем запрос к БД
    # В зависимости от типа медиа меняется первая строка в запросе - поля, которые нам нужны для таблицы Фактов
    # названия полей находятся в справочнике config_tv_investments.first_row_query_dict
    # Фильтрация строк для ВСЕХ одинаковая производится по условиям config_tv_investments.main_filter_str
    query = f"""
    select t11.vid, t1.cid, t1.distr as adDistributionType, t1.mon, t1.from_mon,  t1.from_cid, t1.estat, 
    t1.cnd_cost_rub as ConsolidatedCostRUB,  t1.vol, t1.cnt, t3.atid as adTypeId, t4.rid, t4.nid as netId
    from tv_Ad_month t1 
    right join (
    select distinct vid from tv_Appendix t10
    where {main_filter}) t11 on t1.vid=t11.vid
    left join tv_Ad t3 on t1.vid=t3.vid
    left join tv_Company t4 on t1.cid=t4.cid
    where t1.mon={str(mon_num)} and t1.cmpn_netLoc='Y'
    """
    # отправляем запрос в БД Медиа инвестиции
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    df['researchDate'] = start_date
    # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
    # Добавляем в датаФрейм - год, тип медиа, тип медиа+ИД объявления, расходы с дисконтом
    df = append_custom_columns(df, report='buying', media_type=config.media_type_dict[media_type_long])
    # Прописываем условия для создания колонки tv_type_ooh_reg
    df = get_tv_type_ooh_reg(df, media_type=media_type_long, report='media')
    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name)

    # оставляем только нужные поля для записив БД
    vars_lst = normalize_lst[2]
    use_cols_lst = [i[:i.find(' ')] for i in vars_lst]
    df = df[use_cols_lst]
    # нормализуем типы данных
    df = normalize_columns_types(df, normalize_lst[0], normalize_lst[1])

    return df


# In[ ]:


"""
Radio расходы - создаем функцию, которая 
- забирает расходы по месяцам
- переименовывает поля (приводит к стандарту ТВ Индекс
Функция принимает на вход
*normalize_lst - передаем, как отдельные параметры - Список полей с типом int / Список полей с типом float
start_mon - дата начала месяца, за который забираем статистику
mon_num - номер месяца по летоисчислению Медиаскоп
media_type - тип медиа, по которому забираем расходы
"""

def get_table_radio_costs(*normalize_lst, media_type, start_date='2023-01-01', mon_num=396, main_filter=None):
    # для запроса к БД приводим тип медиа к нижнему регистру
    media_type = media_type.lower()
    media_type_long = config.media_type_dict[media_type]
    # Формируем запрос к БД
    # В зависимости от типа медиа меняется первая строка в запросе - поля, которые нам нужны для таблицы Фактов
    # названия полей находятся в справочнике config_tv_investments.first_row_query_dict
    # Фильтрация строк для ВСЕХ одинаковая производится по условиям config_tv_investments.main_filter_str
    query = f"""
    select t11.vid, t1.stid as cid, t1.distr as adDistributionType, t1.mon, t1.from_mon,  t1.from_stid as from_cid, t1.estat, 
    t1.cost_rub as ConsolidatedCostRUB,  t1.vol, t1.cnt, t3.atid as adTypeId, t4.rid, t4.hlid as netId
    from ra_Ad_month t1 
    right join (
    select distinct vid from ra_Appendix t10
    where {main_filter}) t11 on t1.vid=t11.vid
    left join ra_Ad t3 on t1.vid=t3.vid
    left join ra_Station  t4 on t1.stid=t4.stid
    where t1.mon={str(mon_num)} and t1.cmpn_netLoc='Y'
    """
    # отправляем запрос в БД Медиа инвестиции
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    df['researchDate'] = start_date
    # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
    # Добавляем в датаФрейм - год, тип медиа, тип медиа+ИД объявления, расходы с дисконтом
    df = append_custom_columns(df, report='buying', media_type=media_type)
    # Прописываем условия для создания колонки tv_type_ooh_reg
    df = get_tv_type_ooh_reg(df, media_type=media_type_long, report='media')
    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name)
    # оставляем только нужные поля для записив БД
    vars_lst = normalize_lst[2]
    use_cols_lst = [i[:i.find(' ')] for i in vars_lst]
    df = df[use_cols_lst]

    # нормализуем типы данных
    df = normalize_columns_types(df, normalize_lst[0], normalize_lst[1])

    return df


# In[ ]:


"""
Outdoor расходы - создаем функцию, которая 
- забирает расходы по месяцам
- переименовывает поля (приводит к стандарту ТВ Индекс
Функция принимает на вход
*normalize_lst - передаем, как отдельные параметры - Список полей с типом int / Список полей с типом float
start_mon - дата начала месяца, за который забираем статистику
mon_num - номер месяца по летоисчислению Медиаскоп
media_type - тип медиа, по которому забираем расходы
"""

def get_table_outdoor_costs(*normalize_lst, media_type, start_date='2023-01-01', mon_num=396, main_filter=None):
    # для запроса к БД приводим тип медиа к нижнему регистру
    media_type = media_type.lower()
    media_type_long = config.media_type_dict[media_type]
    # Формируем запрос к БД
    # В зависимости от типа медиа меняется первая строка в запросе - поля, которые нам нужны для таблицы Фактов
    # названия полей находятся в справочнике config_tv_investments.first_row_query_dict
    # Фильтрация строк для ВСЕХ одинаковая производится по условиям config_tv_investments.main_filter_str
    query = f"""
    select t11.vid, t1.agid as cid, '' as adDistributionType, t1.mon, t1.mon as from_mon,  t1.agid as from_cid, t1.estat, 
    t1.cost_rub as ConsolidatedCostRUB,  t1.vol, t1.cnt, t1.agtid as adTypeId, t1.rid, t5.nid as netId
    from od_Ad_month t1 
    right join (
    select distinct vid from od_Appendix t10
    where {main_filter}) t11 on t1.vid=t11.vid
    left join od_Agency  t4 on t1.agid=t4.agid
    left join od_Network t5 on t4.nid=t5.nid
    where t1.mon={str(mon_num)} and t1.cmpn_netLoc='Y'
    """
    # отправляем запрос в БД Медиа инвестиции
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    df['researchDate'] = start_date
    # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
    # Добавляем в датаФрейм - год, тип медиа, тип медиа+ИД объявления, расходы с дисконтом
    df = append_custom_columns(df, report='buying',media_type=media_type)
    # Прописываем условия для создания колонки tv_type_ooh_reg
    df = get_tv_type_ooh_reg(df, media_type=media_type_long, report='media')
    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name) 

    # оставляем только нужные поля для записив БД
    vars_lst = normalize_lst[2]
    use_cols_lst = [i[:i.find(' ')] for i in vars_lst]
    df = df[use_cols_lst]
    # нормализуем типы данных
    df = normalize_columns_types(df, normalize_lst[0], normalize_lst[1])

    return df


# In[ ]:


"""
Pess расходы - создаем функцию, которая 
- забирает расходы по месяцам
- переименовывает поля (приводит к стандарту ТВ Индекс
Функция принимает на вход
*normalize_lst - передаем, как отдельные параметры - Список полей с типом int / Список полей с типом float
start_mon - дата начала месяца, за который забираем статистику
mon_num - номер месяца по летоисчислению Медиаскоп
media_type - тип медиа, по которому забираем расходы
"""

def get_table_press_costs(*normalize_lst, media_type, start_date='2023-01-01', mon_num=396, main_filter=None):
    # для запроса к БД приводим тип медиа к нижнему регистру
    media_type = media_type.lower()
    media_type_long = config.media_type_dict[media_type]
    # Формируем запрос к БД
    # В зависимости от типа медиа меняется первая строка в запросе - поля, которые нам нужны для таблицы Фактов
    # названия полей находятся в справочнике config_tv_investments.first_row_query_dict
    # Фильтрация строк для ВСЕХ одинаковая производится по условиям config_tv_investments.main_filter_str
    query = f"""
    select t11.vid, t1.eid as cid, t1.alloc as adDistributionType, t1.mon, t1.mon as from_mon,  t1.eid as from_cid, t1.estat, 
    t1.cost_rub as ConsolidatedCostRUB,  t1.vol, t1.cnt, t4.atid as adTypeId, t5.rid, t1.phid as netId
    from pr_Ad_month t1 
    right join (
    select distinct vid from pr_Appendix t10
    where {main_filter}) t11 on t1.vid=t11.vid
    left join pr_Ad  t4 on t1.vid=t4.vid
    left join pr_Edition t5 on t1.eid=t5.eid
    where t1.mon={str(mon_num)} and t1.cmpn_netLoc='Y'
    """
    # отправляем запрос в БД Медиа инвестиции
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    df['researchDate'] = start_date
    # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
    # Добавляем в датаФрейм - год, тип медиа, тип медиа+ИД объявления, расходы с дисконтом
    df = append_custom_columns(df, report='buying', media_type=media_type)
    # Прописываем условия для создания колонки tv_type_ooh_reg
    df = get_tv_type_ooh_reg(df, media_type=media_type_long, report='media')
    # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
    # перебираем справочник config_tv_investments.rename_cols_dict
    # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
    # чтобы передать для присвоения нового названия
    new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
    df = df.rename(columns=new_cols_name)       
    # оставляем только нужные поля для записив БД
    vars_lst = normalize_lst[2]
    use_cols_lst = [i[:i.find(' ')] for i in vars_lst]
    df = df[use_cols_lst]
    # нормализуем типы данных
    df = normalize_columns_types(df, normalize_lst[0], normalize_lst[1])

    return df


# In[ ]:


# media_type = 'tv'
# media_type_long = 'tv'
# main_filter=config_media_costs.main_filter_str
# mon_num = 420
# start_date = '2025-01-01'


# In[ ]:


"""
Функция для перезаписи кастомных значений в таблицах расходов Медиа инветсиций
Может возникнуть такая необходимость, например - пересмотрели размер дисконта / изменили правила опеределения одной из кастомных колонок и тд.
В этом случае забираем из нашей БД из каждой таблицы расходов колонки, которые мы взяли из Медиаскопа
И пропускаем по тем же шагам обработки, которые применялись в первоначальной загрузке
Таким образом нет необходимости заново обращаться в Медиаскоп - переписываем все на месте
По итогу - удаляем существующую таблицу в БД и на ее место записываем таблицу с новуми данными
"""
def update_media_cost(start_date='', end_date='', media_type='tv'):
    # для запроса к БД приводим тип медиа к нижнему регистру
    media_type = media_type.lower()
    media_type_long = config.media_type_dict[media_type].lower()

    if not start_date:
        # получаем минимальную дату из таблицы Фактов
        query = f"""select min(researchDate) from media_{media_type_long}_costs"""
        # в ответ от БД приходит датаФрейм, поэтому добавляем iloc, чтобы получить строку
        start_date = get_mssql_table(config.db_name, query=query).iloc[0][0]
    start_date = datetime.strptime(str(start_date), '%Y-%m-%d').date()

    if not end_date:
        # получаем последнюю дату из таблицы Фактов
        query = f"""select max(researchDate) from media_{media_type_long}_costs"""
        end_date = get_mssql_table(config.db_name, query=query).iloc[0][0]
    end_date = datetime.strptime(str(end_date), '%Y-%m-%d').date()

    # считаем кол-во дней в периоде
    # каждый день мы будем забирать по отдельности и записывать его в БД
    count_days = (end_date - start_date).days
    count_months = ceil(count_days/30)
    print()
    print(f'Загружаем отчет за период {start_date} - {end_date}. Общее количество месяцев: {count_months+1}')
    print(sep_str)
    print()

     # проходимся по общему количеству дней
    for i in range(count_months):
        # формируем отдельную дату для загрузки
        cur_date = str(start_date + relativedelta(months=i))

        # Формируем запрос к БД - для всех таблиц расходов Медиаинвестиции поля одинкаовые
        query = f""" select adId as vid, cid, adDistributionType, mon_num as mon, from_mon,  from_cid, estat, 
                        ConsolidatedCostRUB,  vol, Quantity as cnt, adTypeId, regionId as rid, netId, researchDate
                    from media_{media_type_long}_costs where researchDate='{cur_date}'
                    """
        # отправляем запрос к БД
        df = get_mssql_table(config.db_name, query=query)
        # вызываем функцию, чтобы добавить новые поля, дисконт и расходы с дисконтом
        # Добавляем в датаФрейм - год, тип медиа, тип медиа+ИД объявления, расходы с дисконтом
        df = append_custom_columns(df, report='buying', media_type=media_type)
        # Прописываем условия для создания колонки tv_type_ooh_reg
        df = get_tv_type_ooh_reg(df, media_type=media_type_long, report='media')
        # переименовываем поля - приводим их в соответсвии с названиями из ТВ Индекс
        # перебираем справочник config_tv_investments.rename_cols_dict
        # если название поля из Медиа инвестиции есть в ключах, то забираем пару ключ-значение
        # чтобы передать для присвоения нового названия
        new_cols_name = {key: value for (key, value) in config_media_costs.rename_cols_dict.items() if key in list(df.columns)}
        df = df.rename(columns=new_cols_name)       
        # забираем из справочника - название таблицы для сохранения / названия полей / поля для нормализации типов данных
        table_name = config_media_costs.media_dicts_costs[media_type][0]
        vars_cols_lst = config_media_costs.media_dicts_costs[media_type][1]
        int_lst = config_media_costs.media_dicts_costs[media_type][2]
        float_lst = config_media_costs.media_dicts_costs[media_type][3]

        # оставляем только нужные поля для записив БД
        use_cols_lst = [i[:i.find(' ')] for i in vars_cols_lst]
        df = df[use_cols_lst]
        # нормализуем типы данных
        df = normalize_columns_types(df, int_lst, float_lst)

        cond = f"researchDate= '{cur_date}'"

        print()
        print(sep_str)
        print(f'Удалем строки из таблицы: {table_name} по условию: {cond}')
        print()
        # удаляем данные за этот день из БД
        removeRowsFromDB(config.db_name, table_name, cond)

        # заливаем новую
        downloadTableToDB(config.db_name, table_name, df)


# In[ ]:





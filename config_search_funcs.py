#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd

import config
import config_media_costs
from db_funcs import get_mssql_russian_chars, get_mssql_table


# In[3]:


"""
нам необходимо из базы Медиаскоп инвестиции получить уникальные ИД суббрендов по УЗКОМУ фильтру t10.sid2 in (2272,2277) начиная с 01.01.2020
далее в запросе для получения статистики мы возьмем более широкий фильтр для t10.sid2 in (2277, 2272, 3972, -11, -126)
и добавим к нему условие, что суббренды нас интересуют только из полученных здесь
таким образом мы сможем уменьшить размер ответа из широкого фильтра

При этом нам нужен общий список суббрендов НЕ зависимо от типа Медиа
т.е. по итогу при получении статистики по ТВ мы будем в т.ч. учитывать ИД суббрендов из прессы, радио и тд.

Эта функция работает при каждом обновлении таким образом, в нее постоянно добавляются новые ИД суббрендов
так же используем ее для каждого типа медиа
"""

def get_subbrand_id_str(mon_num='360'):
    media_type_lst=['tv', 'ra', 'od', 'pr']
    result = pd.DataFrame()
    for media_type in media_type_lst:
        # создаем фильтр, который будем применять во воложенном запросе для фильтрации ВСЕХ ТАБЛИЦ по ВСЕМ ИСТОЧНИКАМ
        # этот фильтр используем в ТВ, Радио, ООН, Пресса
        # Префикс для фильтруемой таблицы задан с запасом = t10
        query = f"""
        select sbid, name from Subbrand
        where sbid in (
        select distinct t3.sbid from
        (select distinct t10.sid2, t10.sbid  from {media_type}_Ad_month t1
        left join {media_type}_Appendix t10
        on t1.vid=t10.vid
        where t1.mon>=360 and t10.sid2 in (2272,2277)) t3)
        """
        # отправляем запрос в БД Медиа инвестиции

        df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)

        result = pd.concat([result, df])

    result =set(result['sbid']) #result.drop_duplicates(['sbid'])
    subbrand_id_str = config.get_lst_to_str(result)

    return subbrand_id_str


# In[ ]:


def get_outdoor_regions():
    # забираем названия городов из гугл докса
    media_tyte_detail_link = config_media_costs.media_type_detail_dict['outdoor']
    df = pd.read_csv(media_tyte_detail_link)
    df['Region'] = df['Region'].str.strip().str.upper()    
    # удалем дубликаты
    regions_name_tup = tuple(set(df['Region']))  
    # отправляем запрос в БД Медиа инвестиции
    query = f"select distinct rid from Region where name in {regions_name_tup}"
    df = get_mssql_russian_chars(config.investments_db_name, query=query, conn_lst=config.conn_lst)
    # оставляем уникальные ИД городов
    regions_id = set(df['rid'])
    return regions_id


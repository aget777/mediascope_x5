#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os

from time import sleep
import config
import config_tv_index
from config_search_funcs import get_subbrand_id_str
from nat_tv_ratings import get_nat_tv_reports
from create_dicts_tv_index import update_tv_index_dicts, update_nat_tv_ad_dict
from google_connector import append_ads_to_google


start_date = '2025-04-01' # если дата начала НЕ указана, то start_date = Сегодня минус 3 дня минус 17 дней
end_date = '2025-04-27' #- ВКЛЮЧИТЕЛЬНО / если дата окончания НЕ указана, то end_date = Сегодня минус 3 дня

flag='regular' #first / regular


# In[2]:


# Если флаг first - создаем набор пустых таблиц-справочников / 
# забираем статистику / заполняем таблицы фактов и справочник nat_tv_ad_dict
# Если флаг regular - то грузим статистику и обновляем справочники
# если start_date(дата начала) НЕ указана, то 
# удаляем из БД - последние 17(сегодня-14-3) дней
# перезаисываем статистику за этот период
# если НЕ указана end_date(дата окончания) - ВКЛЮЧИТЕЛЬНО, то грузим данные до сегодня-3 дня
# получаем статистику по отчету Simple и Buying
get_nat_tv_reports(start_date=start_date, end_date=end_date, flag=flag)
# get_nat_tv_reports(flag=flag)
# Обновляем все справочники из ТВ Индекс
update_tv_index_dicts()
# Обновляем основной справочник объявлений
update_nat_tv_ad_dict()

# загружаем в гугл докс Чистка список объяалений с флагом=2
query = config_tv_index.google_new_ads_nat_tv['query']
worksheet = config_tv_index.google_new_ads_nat_tv['worksheet']

append_ads_to_google(query=query, worksheet=worksheet)


# In[ ]:





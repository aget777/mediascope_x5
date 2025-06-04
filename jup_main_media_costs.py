#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os

from time import sleep
import config
import config_media_costs
from config_search_funcs import get_subbrand_id_str
from media_costs import get_media_costs_report, get_mon_num_from_date
from create_dicts_adex import create_adex_tables, download_adex_default_dicts, update_media_ads_dict, update_another_dicts, update_list_dicts, create_adex_views
from google_connector import append_ads_to_google


# subbrand_id_str = get_subbrand_id_str(mon_num='360')
# special_ad_filter = f'{config_media_costs.main_filter_str} AND t10.sbid in ({subbrand_id_str})'

start_date = '2024-05-01' # если дата начала НЕ указана, то start_date = Сегодня минус 3 дня минус 17 дней
end_date = '2025-02-02' #- ВКЛЮЧИТЕЛЬНО / если дата окончания НЕ указана, то end_date = Сегодня минус 3 дня

flag='regular' #first / regular


# In[ ]:





# In[ ]:





# In[2]:


# создаем пустые таблицы Фактов и Справочники
# Заполняем справоники по умолчанию данными config_media_costs.adex_defult_dicts
# Company / Ad_type / EState / 
if flag=='first':
    # создаем все пустые таблицы
    create_adex_tables()
    sleep(10)
    # наполняем данными справочники, которые НЕ обновляются
    download_adex_default_dicts()
    sleep(10)
    # создаем Представления
    create_adex_views()

# забираем статистику по каждому Медиа за указанный период
for key, value in config.media_type_dict.items():
    get_media_costs_report(start_date=start_date, end_date=end_date, media_type=key, flag=flag)


# In[3]:


for media_type, media_type_long in config.media_type_dict.items():
    update_media_ads_dict(media_type)
    update_another_dicts(media_type)
    update_list_dicts(media_type)


# In[4]:


# загружаем в гугл докс Чистка список объяалений с флагом=2
query = config_media_costs.google_new_ads_media_invest['query']
worksheet = config_media_costs.google_new_ads_media_invest['worksheet']

append_ads_to_google(query=query, worksheet=worksheet)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





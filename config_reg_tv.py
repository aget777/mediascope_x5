#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


import config
import config_tv_index


# In[2]:


"""
Словарь регионов для загрузки статистики по городам РФ
Взяли из справки ТВ Индекс
https://github.com/MEDIASCOPE-JSC/mediascope-jupyter/blob/main/tv_index/simple_breaks_cities_01_all_breaks_one_day_one_channel.ipynb
"""
regions_dict = {
    40: 'БАРНАУЛ',
    18: 'ВЛАДИВОСТОК',
    5: 'ВОЛГОГРАД',
    8: 'ВОРОНЕЖ',
    12: 'ЕКАТЕРИНБУРГ',
    25: 'ИРКУТСК',
    19: 'КАЗАНЬ',
    45: 'КЕМЕРОВО',
    23: 'КРАСНОДАР',
    17: 'КРАСНОЯРСК',
    1: 'МОСКВА',
    4: 'НИЖНИЙ НОВГОРОД',
    15: 'НОВОСИБИРСК',
    21: 'ОМСК',
    14: 'ПЕРМЬ',
    9: 'РОСТОВ-НА-ДОНУ',
    6: 'САМАРА',
    2: 'САНКТ-ПЕТЕРБУРГ',
    10: 'САРАТОВ',
    39: 'СТАВРОПОЛЬ',
    3: 'ТВЕРЬ',
    55: 'ТОМСК',
    16: 'ТЮМЕНЬ',
    20: 'УФА',
    26: 'ХАБАРОВСК',
    13: 'ЧЕЛЯБИНСК',
    7: 'ЯРОСЛАВЛЬ'
}


# In[ ]:





# In[6]:


"""
Здесь прописываем логические условия ad_filter 
для Оегионального ТВ убираем условие adDistributionType IN (N,O)
т.к. нам нужны все варианты вещания
"""

reg_tv_ad_filter = config.nat_tv_ad_filter[:config.nat_tv_ad_filter.find(' and adDistributionType IN (N,O)')]


# In[ ]:





# In[4]:


"""
Указываем список статистик для расчета
"""

# Статистики для отчета Simple
reg_tv_simple_statistics = config_tv_index.nat_tv_simple_statistics 

# Статистики для отчета Buying
reg_tv_bying_statistics = config_tv_index.nat_tv_bying_statistics


# In[5]:


"""
Срезы для отчета Simple и Buying по сути одинаковые
Поэтому задаем их только 1 раз здесь
ЗДЕСЬ НИЧЕГО НЕ МЕНЯЕМ 
эти срезы зашиты в таблицы БД, если их изменить
Создаем список срезов по Nat_tv
Указываем список срезов - в задаче не может быть больше 25 срезов
"""

reg_tv_slices = config_tv_index.nat_tv_slices
reg_tv_buying_slices = config_tv_index.nat_tv_buying_slices


# In[6]:


"""
Задаем опции расчета
kitId - набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow)
Перебираем правила из ТВ Индекс и меняем значение для kitId  - ставим 3
"""

reg_tv_options = {key: (3 if key=='kitId' else value) for key, value in config_tv_index.nat_tv_options.items()}


# In[7]:


"""
Берем правила Таргетингов из ТВ индекс
Меяем название группы доходов, чтобы оно соответсвовало названиям из Гродов России
cats.get_tv_grp_type()
"""

reg_tv_targets = dict([(k, v.replace('incomeGroupRussia', 'incomeGroup')) for k, v in config_tv_index.nat_tv_targets.items()])


# In[8]:


"""
таблица фактов по отчету Simple

При создании 2-х таблиц БД - отчет Simple / отчет Buying
используем все, что указанов ТВ индекс, ничего не меняем
"""

reg_tv_simple = 'reg_tv_simple'

reg_tv_simple_vars_list = config_tv_index.nat_tv_simple_vars_list
reg_tv_simple_int_lst = config_tv_index.nat_tv_simple_int_lst
reg_tv_simple_float_lst = config_tv_index.nat_tv_simple_float_lst


# In[9]:


"""
таблица фактов по отчету Buying
используем все, что указанов ТВ индекс, ничего не меняем
"""

reg_tv_buying = 'reg_tv_buying'

reg_tv_buying_vars_list = config_tv_index.nat_tv_buying_vars_list

reg_tv_buying_int_lst = config_tv_index.nat_tv_buying_int_lst 
reg_tv_buying_float_lst = config_tv_index.nat_tv_buying_float_lst


# In[ ]:





# In[10]:


"""
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
"""

reg_tv_fact = {
    'simple': [reg_tv_simple, reg_tv_simple_vars_list, reg_tv_simple_int_lst, reg_tv_simple_float_lst],
    'buying': [reg_tv_buying, reg_tv_buying_vars_list, reg_tv_buying_int_lst, reg_tv_buying_float_lst],
}


# In[11]:


"""
словарь с характеристиками Объявлений
забираем через отчет Simple

пересоздаем пустую таблицу Справочников в БД
"""

reg_tv_ad_dict = 'reg_tv_ad_dict'

reg_tv_ad_dict_vars_list = config_tv_index.nat_tv_ad_dict_vars_list

reg_tv_ad_dict_int_lst = config_tv_index.nat_tv_ad_dict_int_lst


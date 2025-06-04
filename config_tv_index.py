#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np


import config


# In[2]:


"""
Здесь прописываем логические условия ad_filter 
Они применяются для получения статистики в отчетах Simple и Buying
"""

nat_tv_ad_filter = config.nat_tv_ad_filter


# In[3]:


# subbrand_id_str = get_subbrand_id_str(mon_num='360', media_type='tv')
# special_ad_filter = f'{config_tv_index.nat_tv_ad_filter} AND  or subbrandId IN ({subbrand_id_str})'


# In[4]:


"""
Указываем список статистик для расчета
"""

# Статистики для отчета Simple
nat_tv_simple_statistics =['Rtg000', 'RtgPer', 'StandRtgPer'] 

# Статистики для отчета Buying
nat_tv_bying_statistics = ['Quantity', 'SalesRtgPer', 'StandSalesRtgPer', 'ConsolidatedCostRUB']


# In[5]:


"""
Срезы для отчета Simple и Buying по сути одинаковые
Поэтому задаем их только 1 раз здесь
ЗДЕСЬ НИЧЕГО НЕ МЕНЯЕМ 
эти срезы зашиты в таблицы БД, если их изменить
Создаем список срезов по Nat_tv
Указываем список срезов - в задаче не может быть больше 25 срезов
"""

nat_tv_slices = [
    'advertiserListId', #Список рекламодателей id
    # 'advertiserListName', 
    'brandListId', #Список брендов id
    # 'brandListName', 
    'subbrandListId', #Список суббрендов id
    # 'subbrandListName', 
    'modelListId', #Список продуктов id
    # 'modelListName',
    'articleList2Id', #Список товарных категорий 2 id
    # 'articleList2Name',
    'articleList3Id', #Список товарных категорий 3 id
    # 'articleList3Name',
    'articleList4Id', #Список товарных категорий 4 id
    # 'articleList4Name', 
    'adId',
    'adName',
    'adNotes',
    'adFirstIssueDate',
    'adSloganAudioId', #Ролик аудио слоган id
    # 'adSloganAudioName',
    'adSloganVideoId', #Ролик видео слоган id
    # 'adSloganVideoName',
    'researchDate',
    'regionId', # ИД Региона или Сетевое вещание
    # 'regionName',
    'adDistributionType', # ИД блок распространения 
    # 'adDistributionTypeName', # блок распространения Локальный / Сетевой / Орбитальный
    # 'tvNetId', # id телесети
    # 'tvNetName',
    'tvCompanyId',
    # 'tvCompanyName',
    'adTypeId', #Ролик тип ID 
    # 'adTypeName',
    'adStandardDuration', 
    # 'adPositionType', #Ролик позиционирование id 
    'adPositionTypeName',
    # 'adPrimeTimeStatusId', #Прайм/ОфПрайм роликов id
    'adPrimeTimeStatusName',
    'adStartTime',
    'adSpotId'
         ]


# In[6]:


"""
Задаем опции расчета
"""

nat_tv_options = {
    "kitId": 1, # набора данных (1-Russia all, 2-Russia 100+, 3-Cities, # 4-TVI+ Russia all, 5-TVI+ Russia 100+, 6-Moscow) 
    # "totalType": "TotalChannels" #база расчета тотал статистик: Total Channels. Возможны опции: TotalTVSet, TotalChannelsThem
    "standRtg" : {
      "useRealDuration" : False,
      "standardDuration" : 20
    }
}


# In[7]:


"""
Задаем все демографические группы, чтобы каждую из них вывести в отдельном поле
Задаем необходимые группы
"""

# targets = {
# 'W 25-54'       :'sex = 2 AND age >= 25 AND age <= 54',
# 'All 18-64'     :'age >= 18 AND age <= 64',
# 'M 25-45 BC'    :'sex = 1 AND age >= 25 AND age <= 45 AND incomeGroupRussia IN (2,3)',
# 'ALL'           :'age >= 4'
# }


nat_tv_targets = {
'ALL 25-45'     :'age >= 25 AND age <= 45',
'All 25-55'     :'age >= 25 AND age <= 55',
'ALL 25+'       :'age >= 25',
'ALL 25-54 BC'  :'age >= 25 AND age <= 54 AND incomeGroupRussia IN (2, 3)',
'W 25-44'       :'sex = 2 AND age >= 25 AND age <= 44',
'All 20-50'     :'age >= 20 AND age <= 50',
'ALL 20-50 BC'  :'age >= 20 AND age <= 50 AND incomeGroupRussia IN (2, 3)',
'All 20-55'     :'age >= 20 AND age <= 55',
'W 25+'         :'sex = 2 AND age >= 25',
'ALL 4+'     :'age >= 4 AND age <= 100',
'ALL 18+'     :'age >= 18 AND age <= 100',
'ALL 25-45 BC'     :'age >= 25 AND age <= 45 AND incomeGroupRussia IN (2, 3)',
'ALL 25-55 BC'     :'age >= 25 AND age <= 55 AND incomeGroupRussia IN (2, 3)',
}


# In[8]:


"""
таблица фактов по отчету Simple

При создании 2-х таблиц БД - отчет Simple / отчет Buying
Большая Часть названий полей повторяется
Чтобы сохранить последовательность, если потребуется быстро заменить какое-то поле в БД сразу в 3-х таблицах
будем использовать список названий полей из Справочника - как базовый и в отчете Simple и Buying - просто добавим отличающиеся поля
"""

nat_tv_simple = 'nat_tv_simple'

nat_tv_simple_vars_list = [
            'prj_name nvarchar(50)',
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'media_type_long nvarchar(40)',
            'media_type_detail nvarchar(20)',
            'adId int',
            'researchDate nvarchar(10)',
            'regionId smallint',
            # 'regionName nvarchar(100)',
            'adDistributionType nvarchar(1)',
            # 'adDistributionTypeName nvarchar(30)',
            # 'tvNetId smallint',
            # 'tvNetName nvarchar(100)',
            'tvCompanyId smallint',
    'ad_type_custom nvarchar(15)',
    'cid_custom nvarchar(15)',
    'tv_type_ooh_reg nvarchar(150)',
            # 'tvCompanyName nvarchar(100)',
            'adTypeId smallint',
            # 'adTypeName nvarchar(100)',
            'adStandardDuration int',
            'adPositionTypeName  nvarchar(50)',
            'adPrimeTimeStatusName nvarchar(50)',
            'adStartTime nvarchar(8)',
            'adSpotId bigint',
            'Rtg000 float',
            'RtgPer float',
            'StandRtgPer float',
]

nat_tv_simple_int_lst = ['adId', 'regionId', 'tvCompanyId', 'adTypeId', 'adStandardDuration', 'adSpotId']
nat_tv_simple_float_lst = ['Rtg000', 'RtgPer', 'StandRtgPer']


# In[31]:


"""
таблица фактов по отчету Buying

Создаем список названий полей и типов данных для таблицы Buying в БД MSSQL
берем все поля из таблицы Simple и убрибаем из них метрики отчета Simple
добавляем метрики отчета Buying
"""

nat_tv_buying = 'nat_tv_buying'
nat_tv_buying_vars_list = [col for col in nat_tv_simple_vars_list if col[:col.find(' ')] not in nat_tv_simple_float_lst]
nat_tv_buying_vars_list = nat_tv_buying_vars_list + ['Quantity int', 'SalesRtgPer float', 'StandSalesRtgPer float', 
                    'ConsolidatedCostRUB float', 'year smallint', 'disc float', 'ConsolidatedCostRUB_disc float']

nat_tv_buying_int_lst = ['adId', 'adStandardDuration', 'adSpotId', 'regionId', 'adTypeId', 'tvCompanyId', 'year', 'Quantity']
nat_tv_buying_float_lst = ['SalesRtgPer', 'StandSalesRtgPer', 'ConsolidatedCostRUB', 'ConsolidatedCostRUB_disc', 'disc']


# In[ ]:





# In[10]:


"""
для срезов по Баинговой аудитории нам НЕ нужно запрашивать разбивку по ArticleList2, 3, 4 / ModelList / BrandList и тд
т.к. это все является характеристиками конкретного объявления и находится в справочнике Объявлений nat_tv_ad_dict
в таблице Баинг у нас есть ИД объявления, это означает, что оно НЕ может быть разбито на какие-то более мелкие части
т.е. нет смысла тянуть доп. характеристики, т.к. они НЕ дадут большей детализации
посему мы исключаем их из запроса к ТВ Индекс
"""

nat_tv_buying_slices = [col[:col.find(' ')] for col in nat_tv_buying_vars_list]
nat_tv_buying_slices = list(set(nat_tv_buying_slices) - set(nat_tv_buying_float_lst) - set(['prj_name', 'media_key_id', 'media_type', 'media_type_long',
                                            'media_type_detail', 'year', 'Quantity', 'cid_custom', 'nid_custom', 'tv_type_ooh_reg', 'ad_type_custom']))


# In[ ]:





# In[11]:


"""
словарь с характеристиками Объявлений
забираем через отчет Simple

пересоздаем пустую таблицу Справочников в БД
"""

nat_tv_ad_dict = 'nat_tv_ad_dict'

nat_tv_ad_dict_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
            'media_type_long nvarchar(40)',
            'adId int',
            'adName nvarchar(100)',
            'adNotes nvarchar(250)',
            'advertiserListId int',
            'brandListId int',
            'subbrandListId int',
            'modelListId int',
            'articleList2Id int',
            'articleList3Id int',
            'articleList4Id int',
            'adFirstIssueDate nvarchar(10)',
            'adSloganAudioId int',
            'adSloganVideoId int',
            'cleaning_flag tinyint',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
    'ad_transcribtion nvarchar(500)',
    'category_1 nvarchar(100)',
    'category_2 nvarchar(100)',
    'category_4 nvarchar(100)',
    'category_5 nvarchar(100)',
    'category_6 nvarchar(100)',
    'category_7 nvarchar(100)',
    'category_8 nvarchar(100)',
    'category_9 nvarchar(100)',
    'category_10 nvarchar(100)',
    'category_11 nvarchar(100)',
    'category_12 nvarchar(100)',
    'category_13 nvarchar(100)',
    'category_14 nvarchar(100)',
    'category_15 nvarchar(100)',
    'category_16 nvarchar(100)',
    'category_17 nvarchar(100)',
    'category_18 nvarchar(100)',
    'category_19 nvarchar(100)',
    'category_20 nvarchar(100)',
    'category_21 nvarchar(100)',
    'category_22 nvarchar(100)',
    'category_23 nvarchar(100)',
    'category_24 nvarchar(100)',
    'category_25 nvarchar(100)',
            ]

nat_tv_ad_dict_int_lst = ['adId', 'advertiserListId', 'brandListId', 'subbrandListId', 'modelListId', 'articleList2Id', 'articleList3Id',
                         'articleList4Id', 'adSloganAudioId', 'adSloganVideoId', 'cleaning_flag']


# In[12]:


"""
Словарь Лист Рекламодателей TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_advertiser_list_dict = 'tv_index_advertiser_list_dict'

tv_index_advertiser_list_dict_vars_list = [
            'advertiserListId int',
            'advertiserListName nvarchar(200)',
            'advertiserListEName nvarchar(200)'
]

tv_index_advertiser_list_dict_int_lst = ['advertiserListId']


# In[13]:


"""
Словарь Лист Брендов TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_brand_list_dict = 'tv_index_brand_list_dict'

tv_index_brand_list_dict_vars_list = [
            'brandListId int',
            'brandListName nvarchar(300)',
            'brandListEName nvarchar(300)'
]

tv_index_brand_list_dict_int_lst = ['brandListId']


# In[14]:


"""
Словарь Лист СубБрендов TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_subbrand_list_dict = 'tv_index_subbrand_list_dict'

tv_index_subbrand_list_dict_vars_list = [
            'subbrandListId int',
            'subbrandListName nvarchar(350)',
            'subbrandListEName nvarchar(350)'
]

tv_index_subbrand_list_dict_int_lst = ['subbrandListId']


# In[15]:


"""
Словарь Лист Model TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_model_list_dict = 'tv_index_model_list_dict'

tv_index_model_list_dict_vars_list = [
            'modelListId int',
            'modelListName nvarchar(350)',
            'modelListEName nvarchar(350)'
]

tv_index_model_list_dict_int_lst = ['modelListId']


# In[16]:


"""
Словарь Лист Article List 2 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list2_dict = 'tv_index_article_list2_dict'

tv_index_article_list2_dict_vars_list = [
            'articleList2Id smallint',
            'articleList2Name nvarchar(300)',
            'articleList2EName nvarchar(300)'
]

tv_index_article_list2_dict_int_lst = ['articleList2Id']


# In[17]:


"""
Словарь Лист Article List 3 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list3_dict = 'tv_index_article_list3_dict'

tv_index_article_list3_dict_vars_list = [
            'articleList3Id smallint',
            'articleList3Name nvarchar(300)',
            'articleList3EName nvarchar(300)'
]

tv_index_article_list3_dict_int_lst = ['articleList3Id']


# In[18]:


"""
Словарь Лист Article List 4 TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_article_list4_dict = 'tv_index_article_list4_dict'

tv_index_article_list4_dict_vars_list = [
            'articleList4Id int',
            'articleList4Name nvarchar(350)',
            'articleList4EName nvarchar(350)'
]

tv_index_article_list4_dict_int_lst = ['articleList4Id']


# In[19]:


# # Словарь Названий Объявлений TV_Index
# # Используем для объединения данных из отчетов Simple / Buying /  Costs

# tv_index_ad_name_dict = 'tv_index_ad_name_dict'

# tv_index_ad_name_dict_vars_list = [
#             'adId int',
#             'adName nvarchar(300)',
#             'adEName nvarchar(300)'
# ]

# tv_index_ad_name_dict_int_lst = ['adId']


# In[20]:


"""
Словарь Аудио Слоган TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_audio_slogan_dict = 'tv_index_audio_slogan_dict'

tv_index_audio_slogan_dict_vars_list = [
            'adSloganAudioId int',
            'adSloganAudioName nvarchar(200)',
            'adSloganAudioNotes nvarchar(200)'
]

tv_index_audio_slogan_dict_int_lst = ['adSloganAudioId']


# In[21]:


"""
Словарь Видео Слоган TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_video_slogan_dict = 'tv_index_video_slogan_dict'

tv_index_video_slogan_dict_vars_list = [
            'adSloganVideoId int',
            'adSloganVideoName nvarchar(200)',
            'adSloganVideoNotes nvarchar(200)'
]

tv_index_video_slogan_dict_int_lst = ['adSloganVideoId']


# In[22]:


"""
Словарь Регионов TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_region_dict = 'tv_index_region_dict'

tv_index_region_dict_vars_list = [
            'regionId smallint',
            'regionName nvarchar(50)',
            'regionEName nvarchar(50)'
]

tv_index_region_dict_int_lst = ['regionId']


# In[23]:


"""
Словарь ТВ Сети TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_tv_net_dict = 'tv_index_tv_net_dict'

tv_index_tv_net_dict_vars_list = [
            'tvNetId smallint',
            'tvNetName nvarchar(100)',
            'tvNetEName nvarchar(100)',
    'nid_custom nvarchar(15)',
]

tv_index_tv_net_dict_int_lst = ['tvNetId']


# In[24]:


"""
Словарь ТВ Компании TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_tv_company_dict = 'tv_index_tv_company_dict'

tv_index_tv_company_dict_vars_list = [
            'tvCompanyId smallint',
            'tvNetId smallint',
    'cid_custom nvarchar(15)',
    'nid_custom nvarchar(15)',
            'regionId tinyint',
            'tvCompanyHoldingId int',
            'tvCompanyMediaHoldingId tinyint',
            'tvThematicId tinyint',
            'tvCompanyGroupId tinyint',
            'tvCompanyCategoryId tinyint',
            'tvCompanyName nvarchar(100)',
            'tvCompanyEName nvarchar(100)',
            'tvCompanyMediaType nvarchar(5)',
            'information nvarchar(50)'
]

tv_index_tv_company_dict_int_lst = ['tvCompanyId', 'tvNetId', 'regionId', 'tvCompanyHoldingId', 'tvCompanyMediaHoldingId', 
                                    'tvThematicId', 'tvCompanyGroupId', 'tvCompanyCategoryId']


# In[25]:


"""
Словарь Типа Объявления TV_Index
Используем для объединения данных из отчетов Simple / Buying /  Costs
"""

tv_index_ad_type_dict = 'tv_index_ad_type_dict'

tv_index_ad_type_dict_vars_list = [
            'adTypeId smallint',
    'ad_type_custom nvarchar(15)',
            'adTypeName nvarchar(60)',
            'adTypeEName nvarchar(60)',
            'notes nvarchar(110)',
            'accountingDurationType nvarchar(5)',
            'isOverride nvarchar(5)',
            'isPrice nvarchar(5)',
            'positionType nvarchar(5)'
]

tv_index_ad_type_dict_int_lst = ['adTypeId']


# In[26]:


"""
Словарь Блок распространения TV_Index 
L - Локальный / N - Сетевой / O - орбитальный / U - N/A
Используем в т.ч. для определения nat_tv, reg_tv и тд.
"""

tv_index_distribution_dict = 'tv_index_distribution_dict'

tv_index_distribution_dict_vars_list = [
            'adDistributionType nvarchar(1)',
            'name nvarchar(50)',
            'ename nvarchar(50)',
]

tv_index_distribution_dict_int_lst = []


# In[27]:


"""
для каждого тип Медиа отдельный ключ tv / ra / od / pr - это префикс из Медиаскопа
Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
"""

nat_tv_fact = {
    'simple': [nat_tv_simple, nat_tv_simple_vars_list, nat_tv_simple_int_lst, nat_tv_simple_float_lst],
    'buying': [nat_tv_buying, nat_tv_buying_vars_list, nat_tv_buying_int_lst, nat_tv_buying_float_lst],
}


# In[28]:


"""
Список параметров словарей ТВ Индекс для создания таблиц в БД и нормализации данных
Название таблицы / Список названий полей  в БД и типы данных / Список целочисденных полей
"""

tv_index_dicts = {
    'advertiserListId': [tv_index_advertiser_list_dict, tv_index_advertiser_list_dict_vars_list, tv_index_advertiser_list_dict_int_lst],
    'brandListId': [tv_index_brand_list_dict, tv_index_brand_list_dict_vars_list, tv_index_brand_list_dict_int_lst],
    'subbrandListId': [tv_index_subbrand_list_dict, tv_index_subbrand_list_dict_vars_list, tv_index_subbrand_list_dict_int_lst],
    'modelListId': [tv_index_model_list_dict, tv_index_model_list_dict_vars_list, tv_index_model_list_dict_int_lst],
    'articleList2Id': [tv_index_article_list2_dict, tv_index_article_list2_dict_vars_list, tv_index_article_list2_dict_int_lst],
    'articleList3Id': [tv_index_article_list3_dict, tv_index_article_list3_dict_vars_list, tv_index_article_list3_dict_int_lst],
    'articleList4Id': [tv_index_article_list4_dict, tv_index_article_list4_dict_vars_list, tv_index_article_list4_dict_int_lst],
    'adSloganAudioId': [tv_index_audio_slogan_dict, tv_index_audio_slogan_dict_vars_list, tv_index_audio_slogan_dict_int_lst],
    'adSloganVideoId': [tv_index_video_slogan_dict, tv_index_video_slogan_dict_vars_list, tv_index_video_slogan_dict_int_lst],

}


# In[29]:


"""
Список словарей, которые НЕ требуют обновления
Они загружается только 1 раз при первой загрузке Проекта
"""

tv_index_default_dicts = {
    'regionId': [tv_index_region_dict, tv_index_region_dict_vars_list, tv_index_region_dict_int_lst],
    'tvNetId': [tv_index_tv_net_dict, tv_index_tv_net_dict_vars_list, tv_index_tv_net_dict_int_lst],
    'tvCompanyId': [tv_index_tv_company_dict, tv_index_tv_company_dict_vars_list, tv_index_tv_company_dict_int_lst],
    'adTypeId': [tv_index_ad_type_dict, tv_index_ad_type_dict_vars_list, tv_index_ad_type_dict_int_lst],
    'adDistributionType': [tv_index_distribution_dict, tv_index_distribution_dict_vars_list, tv_index_distribution_dict_int_lst]
}


# In[30]:


"""
Прописываем условия для сохранения ИД и характеристик новых объявлений в Гугл докс
query - параметры запроса к нашей БД, чтобы сформировать нужную таблицу
worksheet - название листа для сохранения
"""



google_new_ads_nat_tv = {
    'db_cols': {
   'media_key_id': 't1.media_key_id',
    'category_1': 't1.media_type_detail as media_type_long',
    'adId': 't1.adId',
    'advertiserListName': 't1.advertiserListName',
    'brandListName': 't1.brandListName',
    'subbrandListName': 't1.subbrandListName',
    'modelListName': 't1.modelListName',
    'articleList2Name': 't1.articleList2Name',
    'articleList3Name': 't1.articleList3Name',
    'articleList4Name': 't1.articleList4Name',
    'adName': 't1.adName',
    'ad_description': 't1.adNotes as ad_description',
    'adSloganAudioName': 't1.audioSloganName', 
    'adSloganVideoName': 't1.videoSloganName',
     'media_type': 't1.media_type_long as media_type',
    'adFirstIssueDate': 't1.adFirstIssueDate',
},

'query' : """
            from
            	(select * from google_cleaning_adex_view
                union all
                select * from google_cleaning_tv_index_view) t1
            """,

'worksheet' : config.new_ads_nat_tv_sheet
}


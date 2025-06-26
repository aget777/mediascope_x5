#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
from datetime import datetime



# моя БД, в которую сохраняем кастомные распарсенные данные
host_mssql = 'REGRU-54856\MSSQLSERVER01'
port_mssql = '1433'

# название БД  MSSQL
db_name = 'mediascope_x5' #'test_media_costs' #'mediascope_dbbs_alfa' #'test_media_costs'

# указываем параметры подключения к БД Медиаскоп - МедиаИнвестиции 
host_investments = 'ttk.igronik.ru'
investments_db_name = 'Media'

# гугл почта, на которой хранится файл с чисткой
gmail = 'analytics.igronik@gmail.com'

# указываем путь к основной папке с доступами
cred_path = r'C:\Users\o.bogomolov\Desktop\Jupyter_notebook\43_Проекты_Рейтинги_и_расходы\02_02_version_3_X5\cred'
# токен гугл
credentials_file = 'premium-ember-449909-c3-c3a505f0a5e7.json'
service = os.path.join(cred_path, credentials_file)

# ОРИГИНАЛ ссылка на словарь чистки в формате CSV
full_cleaning_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vSY8rjZ9jc6UsUTpGIwelL3wq1Q0T74VY-w2XSbZYiVAnpDn4N3JUJ_vXvyq5CjxqSFOkHVFVnCGvop/pub?gid=1104698177&single=true&output=csv'
# # ОРИГИНАЛ вот этот гугл докс 
google_docs_link = 'https://docs.google.com/spreadsheets/d/1V2ffTUYtpDInt2ahrSDvmaHsuNHU4_LByoGQVBN2hv4/edit?usp=sharing'


# CSV ссылки на гугл докс, в котором определяем правила для ТВ нат / ТВ рег / ТВ Споносор и тд
# словарь для применения этих правил создаем непосредственно в файлах config_media_costs
# ссылка на сам гугл докс https://docs.google.com/spreadsheets/d/1HbXGzGvuxXerDULNctOsonlbosz9NNYHA8LJz0jHyyk/edit?usp=sharing
# segments_main_link = ''
# tv_net_group_link = ''
media_type_detail_tv_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTorbWqwUcy1Wm8Q66y39VPB7YYthlpLnoLOou73jVnu-Iq0yz-VIL0SJVedCusyxLcEnEEmUfzaP5x/pub?gid=536314515&single=true&output=csv'
media_type_detail_outdoor_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTorbWqwUcy1Wm8Q66y39VPB7YYthlpLnoLOou73jVnu-Iq0yz-VIL0SJVedCusyxLcEnEEmUfzaP5x/pub?gid=0&single=true&output=csv'



# название листа, на котрый выгружаем новые объявления
new_ads_nat_tv_sheet = 'new_ads'
new_ads_media_invest_sheet = new_ads_nat_tv_sheet
# ссылка в формате CSV на общую таблицу дисконтов по всем типам медиа - одна для всех проектов
discounts_link = 'https://docs.google.com/spreadsheets/d/e/2PACX-1vTeV7y_TRGzF1gZvlyz43hhtYq_v_-9URsPVT-TdoGLebYZcLHgH83EAzqCAj1OhN9wHfFLMu3_DSzz/pub?gid=0&single=true&output=csv'


# In[2]:


"""
создаем функцию, чтобы забрать данные для подключения из текстового файла
"""

def get_cred_dict(file_path, file_name):
    dict = {}
    creds = open(os.path.join(file_path, file_name), encoding='utf-8').read()
    for text in creds.split('\n'):
        content = text.split(': ')
        dict[content[0]] = content[1]

    return dict


# In[3]:


db_mssql_file = 'db_mssql_creds.txt'
db_mssql_dict = get_cred_dict(cred_path, db_mssql_file)

db_mssql_login = db_mssql_dict['login']
db_mssql_pass = db_mssql_dict['pass']


# In[4]:


db_investments_file = 'db_invetments_creds.txt'
db_investments_dict = get_cred_dict(cred_path, db_investments_file)

db_investments_login = db_investments_dict['login']
db_investments_pass = db_investments_dict['pass']

conn_lst = [host_investments, port_mssql, db_investments_login, db_investments_pass]


# In[5]:


"""
Функция для того, чтобы объеденить все элементы списка в одну строку
это нужно для условий фильтрации в условиях запроса TV_index
"""

def get_lst_to_str(lst):
    return ', '.join(str(elem) for elem in lst)


# In[6]:


"""
Здесь передаем список ИД для фильтрации по ad_filter 
Услоия этого фильтра представлены в строке ниже
"""

article_lev_4_id_lst = [2408, 4780, 4926, 5028]
article_lev_4_id_str = get_lst_to_str(article_lev_4_id_lst)

subbrand_id_lst = [137166, 489562, 494633, 556193, 872285, 966983, 1155675, 1245528]
subbrand_id_str = get_lst_to_str(subbrand_id_lst)


# In[ ]:





# In[6]:


"""
создаем словарь, который будем использовать 
- для правильного сопоставления названий полей в Гугл доксе чистка и названий полей из нашей БД
- для того, чобы распложить поля именно так, как они должны идти в Гугл доксе

Ключ - это название поля в Гугл доксе (вторая строка)
Значение - это название поля в нашей БД
Порядок указываем слева направо, как поля должны идти в Гугл доксе
"""

google_cols = { 
    'db_cols': {
    'media_key_id': 't1.media_key_id',
    'media_type': 't1.media_type_long as media_type',
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
    # 'media_type': 't1.media_type_long as media_type',
    'adFirstIssueDate': 't1.adFirstIssueDate',}
}


# In[ ]:





# In[ ]:





# In[12]:





# In[ ]:





# In[5]:


"""
Медиа для загрузки - ТОЛЬКО медиа для получения статистики
Словарь типов медиа, которые используем для загрузки статистики из Медиаскоп Инвестиции
здесь указываем только те медиа, по которым НУЖНА статистика
ключ - это короткое название медиа в БД медиаскоп (оно подставляется в таблицы справочники и факты)
значение - это длинное название медиа - оно используется в нашей БД mssql 
- подставляется в название справочников и фактов
- используется для связи с таблицей Дисконтов и Чистки
# {'tv': 'tv', 'ra': 'radio', 'od': 'outdoor', 'pr': 'press'}
"""

media_type_dict = {'tv': 'tv', 'ra': 'radio', 'od': 'outdoor', 'pr': 'press'}


# In[8]:


"""
Словарь типов ВСЕХ медиа из Медиаскоп Инвестиции
здесь указываем все медиа для создания пустых таблиц в БД при создании нового проекта
ключ - это короткое название медиа в БД медиаскоп (оно подставляется в таблицы справочники и факты)
значение - это длинное название медиа - оно используется в нашей БД mssql 
- подставляется в название справочников и фактов
- используется для связи с таблицей Дисконтов и Чистки

"""

media_type_full_dict = {'tv': 'tv', 'ra': 'radio', 'pr': 'press', 'od': 'outdoor'}


# In[9]:


"""
Здесь прописываем логические условия ad_filter 
Они применяются для получения статистики в отчетах Simple и Buying
"""

nat_tv_ad_filter = f'(articleLevel4Id IN ({article_lev_4_id_str}) or subbrandId IN ({subbrand_id_str})) and adIssueStatusId=R and adDistributionType IN (N,O)'


# In[10]:


"""
создаем фильтр, который будем применять во воложенном запросе для фильтрации ВСЕХ ТАБЛИЦ по ВСЕМ ИСТОЧНИКАМ
этот фильтр используем в ТВ, Радио, ООН, Пресса
Префикс для фильтруемой таблицы задан с запасом = t10
"""

main_filter_str = f't10.sid4 in ({article_lev_4_id_str}) or t10.sbid in ({subbrand_id_str})'


# In[11]:


# этот список используем, чтобы добавить единые кастомные ключи, котороые можно будет использовать для всех типов медиа
# Наример tv_123

custom_cols_dict = {'cid': 'cid_custom', 'netId': 'nid_custom', 'adTypeId': 'ad_type_custom', 'tvCompanyId': 'cid_custom'}   


# In[12]:


"""
Справочник Объявлений с собственными названиями
сотрудники ведут гугл докс с чисткой
в этом же файле добавили новые поля с собственными группировками
загружаем эту кастомную таблицу в БД, чтобы потом использовать в дашборде
"""

custom_ad_dict = 'custom_ad_dict'

custom_ad_dict_vars_list = [
            'media_key_id nvarchar(40)',
            'media_type nvarchar(10)',
    # 'media_type_long nvarchar(40)',
     # 'media_type_detail nvarchar(20)',
            # 'adId int',
    #         'advertiserListName nvarchar(150)',
    #         'brandListName nvarchar(300)',
    #         'subbrandListName nvarchar(300)',
    #         'modelListName nvarchar(500)',
    #         'articleList2Name nvarchar(300)',
    #         'articleList3Name nvarchar(400)',
    #         'articleList4Name nvarchar(500)',
    #         'adName nvarchar(200)',
    #         'ad_description nvarchar(500)',
    # 'audioSloganName nvarchar(100)',
    # 'videoSloganName nvarchar(100)',
    #         'adFirstIssueDate nvarchar(10)',
    'tv_type_ooh_reg nvarchar(150)',
    'ad_transcribtion nvarchar(500)',
            'advertiser_type nvarchar(20)',
            'advertiser_main nvarchar(100)',
            'brand_main nvarchar(150)',
            'include_exclude nvarchar(15)',
            'cleaning_flag tinyint',
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

custom_ad_dict_int_lst = ['cleaning_flag']


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





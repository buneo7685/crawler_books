#!/usr/bin/env python
# coding: utf-8

# # Import

# In[1]:


import re
import os
import pandas as pd
import datetime
import time
from natsort import natsorted
from airflow.models import Variable
import numpy as np
from Crawler import ETL_utils

import psycopg2
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
pd.set_option('display.max_rows',10000)
pd.set_option('display.max_colwidth',1000)
pd.set_option('display.max_columns',50)

import warnings
warnings.simplefilter(action='ignore', category=Warning)


try :
    if os.path.split(os.path.abspath(__file__))[1].split('.')[1] == 'py':
        module = 'prd'
    else :
        module = 'dev'
except NameError :    
        module = 'dev'


# # DB Connection

# In[2]:


# Cassandra
cluster = Cluster(contact_points = ['172.17.0.2'] ,  port = '9042')
session = cluster.connect()
session.set_keyspace('books')
session.row_factory = dict_factory

# Postgres
pg_password = Variable.get('Postgres_password')
conn = psycopg2.connect(host = '127.0.0.1' , port = '5432' , user = 'buneoliou' , password = pg_password , database = 'crawler')
cur = conn.cursor()


# # Parameters

# In[58]:


ETL_target = 'home_page'
query_insert_time = session.execute(f"select max(insert_time) from {ETL_target} allow filtering").all()
ETL_target_datetime = pd.DataFrame(query_insert_time).iloc[0,0]

try:
    os.mkdir(f'/home/buneoliou/Crawler/Project_Books/dev_test_files/{ETL_target_datetime}')
except FileExistsError :
    pass


# In[59]:


with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
    param = eval(f.read())['table_param_dict'][ETL_target]

table_name = param['table_name']


# # ETL

# ## 判斷在Cassandra最大insert_time是否存在於Pivot_table

# In[60]:


if pd.read_sql(f"select count(*) from pivot_table where url = '{table_name}' and insert_time = '{ETL_target_datetime}'" , con = conn)['count'].iloc[0] > 0 :
    raise ValueError('Data Exists')
    


# ## Extract : Cassandra指定時間資料

# In[61]:


# 載入cassandra完整資料
result = session.execute(f"select * from {table_name} where insert_time = '{ETL_target_datetime}' allow filtering ").all()
origin_df = pd.DataFrame(result).fillna('')


# In[62]:


if origin_df.shape != (0,0):
    pass
else :
    raise ValueError('No data of the specific time in DB')


# In[63]:


# Filter : 第一階段篩選
origin_mask_1 = origin_df['value'] != ''
origin_mask_2 = origin_df['value'].str.contains('javascript',regex = True , na = False) == False
origin_mask_3 = origin_df['datatype'] != 'src'

origin_df = origin_df[origin_mask_1 & origin_mask_2 & origin_mask_3 ]


# ## Extract : 手動分類檔案 ( gp1 , gp2 )

# In[64]:


class_df = pd.read_csv('/home/buneoliou/Crawler/Project_Books/doc/group_mapping.csv',keep_default_na = False , na_values = 'Error' , names = ['group_lv1','group_lv2','CLASS'] )
gp2 = class_df[class_df['group_lv1'] == ''].drop('group_lv1',axis = 1 )
gp1 = class_df.drop(gp2.index).drop('group_lv2' , axis = 1)


# ## Join : 將 gp1 & gp2 合併至Main

# In[65]:


# Join group_lv1至main
main = pd.merge(origin_df , gp1 , how = 'left' , on = 'group_lv1')

# delete origin_df
if module == 'prd':
    del origin_df

# 以迴圈判斷main的xpath是否包含group_lv2的xpath，符合則修改分類
main['sub_class'] = ''
for idx in range(len(gp2)):
    row = gp2.iloc[idx,:]
    target_idx = main[main.xpath.str.contains(row.group_lv2  ,regex = False)].index
    main.loc[target_idx,'group_lv2'] = row['group_lv2']
    main.loc[target_idx,'sub_class'] = row['CLASS']


# ## Create Table : CLASS非全站分類

# In[66]:


main_df = main[main['sub_class'] == '']
main_df = main_df[main_df['datatype'].isin(['href','innerHTML','text','alt'])]


# ## Create Table : CLASS全站分類

# In[67]:


sub_df = main[main['sub_class']!= '']

if module == 'prd':
    del main

sub_df = sub_df[['insert_time','xpath','datatype','value','sub_class','group_lv2']]
sub_df = sub_df[sub_df['datatype'].isin(['href','innerHTML','text'])]

# Get : 各xpath下datatype數量
sub_df['cnt_value'] = sub_df.groupby(['xpath','value'])['datatype'].transform('count')

# Get : 各xpath下資料數量
sub_df['cnt_xpath'] = sub_df.groupby('xpath')['xpath'].transform('count')

# Get : 各sub_class下的各value值的數量
sub_df['cnt_group_value'] = sub_df[['sub_class','value']].groupby(['sub_class','value'])['value'].transform('count')

# Filter : 排除xpath只有一個值,且其值與其它xpath值重覆
cnt_group_value_mask1 = sub_df['cnt_group_value'] >= 2
cnt_group_value_mask2 = sub_df['cnt_xpath'] ==1
sub_df.drop(index = sub_df[cnt_group_value_mask1 & cnt_group_value_mask2].index , inplace = True)


cnt_mask1 = sub_df['cnt_value'] == 1
cnt_mask2_1 = sub_df['cnt_value'] >= 2
cnt_mask2_2 = sub_df['datatype'] == 'innerHTML'

#Filter : 留下 1. value值未重覆的xpath 
#Filter : 留下 2. 相同的值出兩次以上只留Datatype為innerHTML
sub_df_log = sub_df
sub_df = sub_df[(cnt_mask2_1 &cnt_mask2_2) | cnt_mask1 ][['insert_time','xpath','datatype','value','sub_class','group_lv2']]


# ## Create Table + Join : 全站分類子分類

# In[12]:


# Get : xpath長度
sub_df['len_xpath'] = sub_df['xpath'].map(lambda x : len(x.split('/')))
# Get : sub_class下最小長度
sub_df['min_sub_class_len_xpath'] = sub_df.groupby('sub_class')['len_xpath'].transform('min')

# Create : 取出最小長度的xpath組成新table
len_xpath_mask1 = sub_df['len_xpath'] == sub_df['min_sub_class_len_xpath']
len_xpath_mask2 = sub_df['datatype'] == 'innerHTML'
new_sub_class = sub_df.sort_values('xpath')[len_xpath_mask1 & len_xpath_mask2][['sub_class','value']]

new_sub_class['value'] += '_' 
new_sub_class = new_sub_class.groupby(['sub_class']).sum()
new_sub_class['value'] = new_sub_class['value'].str[:-1]
new_sub_class.columns = ['new_sub_class']

sub_df = pd.merge(sub_df , new_sub_class , how = 'left' , left_on = 'sub_class' , right_on = new_sub_class.index)

if module == 'prd':
    del new_sub_class


# ## Add : Pivot Table時使用的sub_group key ( num_group )

# In[13]:


# Get : 依xpath排序後的key
sort_xpath = pd.DataFrame([(k,v) for k , v in enumerate(natsorted(sub_df.xpath.unique().tolist()))] , columns = ['key','xpath'] )
sub_df = sub_df.merge(sort_xpath , on = 'xpath',how = 'left')
sub_df['key'] = sub_df['key'].astype('object')

if module == 'prd':
    del sort_xpath

# Get : num_group為Pivot Table時sort key
sub_df.sort_values(['new_sub_class','key'] , inplace = True)
sub_df['num_group'] = 1

for i in range(len(sub_df)):
    
    num_before = sub_df.iloc[i-1]['num_group']
    
    if i != 0 :
        if sub_df.iloc[i]['new_sub_class'] == sub_df.iloc[i-1]['new_sub_class']:
            
            if sub_df.iloc[i]['key'] != sub_df.iloc[i-1]['key'] :        
                sub_df.iloc[i,-1] = num_before +1
                
            else :
                sub_df.iloc[i,-1] = num_before                
                
        else :
            sub_df.iloc[i,-1] = 1


# ## Insert DB : 全站分類

# In[14]:


# Insert : 將sub_df轉為json存入cassandra
sub_df_json = sub_df.to_json(orient = 'split')
sub_df_date = ETL_target_datetime
sub_df_pivot = table_name + '_sub'
session.execute('insert into pivot_table_log (insert_time , pivot_table , detail) values (%s,%s,%s) ' , (sub_df_date , sub_df_pivot , sub_df_json))

if module == 'prd':
    del sub_df_json

# 產出Pivot Table並調整,增加缺少欄位
sub_df['value'] += '||'
sub_pivot = sub_df.pivot_table(index = ['insert_time','new_sub_class','num_group'] , columns = 'datatype' , values = 'value' , aggfunc='sum' , fill_value = '')
sub_pivot.reset_index(inplace = True)
sub_pivot['pivot_class'] = '全站分類'
sub_pivot['url'] = table_name
sub_pivot = sub_pivot[['insert_time','pivot_class','new_sub_class','num_group','href','innerHTML','url']]

#Insert : 寫入pivot_table
for _ , data in sub_pivot.iterrows():
    cur.execute("insert into pivot_table (insert_time , pivot_class , group_lv2 , key_gp2 , href , text , url )                 values (%s,%s,%s,%s,%s,%s,%s)" , data)
cur.execute('commit')

if module != 'prd':
    sub_pivot.to_csv(f'dev_test_files/{ETL_target_datetime}/{table_name}_sub_df.csv' , index = False)
else :
    del sub_pivot , sub_df

cnt_sub_df_in_db = pd.read_sql(f"select count(*) from pivot_table where url = '{table_name}' and insert_time = '{ETL_target_datetime}'" , con = conn)['count'][0]
print("SUB STAGE : *{0}* {1} amounts data in Pivot_table".format(table_name,cnt_sub_df_in_db))


# ## Insert DB : 手動分類下CLASS為Null

# In[15]:


main_df = ETL_utils.insert_unmatched_log(df = main_df , con = conn , table_name = table_name )


# ## Insert DB : 手動分類下Ignore

# In[16]:


main_df = ETL_utils.insert_ignore_log(df = main_df , con = conn , table_name = table_name )


# ## Add : xpath用natsorted排序取得row number

# In[17]:


sort_main_xpath = [(k,v) for k ,v in enumerate(natsorted(main_df.xpath.tolist()))]
sort_main_xpath_df = pd.DataFrame(sort_main_xpath , columns = ['key','xpath'])
main_df = main_df.merge(sort_main_xpath_df , on = 'xpath' , how = 'left')


# ## Alter Value : 將結尾為a/alt的value歸類為上層a的值

# In[18]:


alt_df = main_df[main_df['datatype'] == 'alt'].reset_index(drop = True)

for i in range(len(alt_df)) :
    
    # Get : 取出xpath，並排除"/alt"結尾
    alt_xpath = alt_df.loc[i,'xpath']
    target_xpath = alt_xpath[:-4]
    
    # 找出符合上層的資料數量
    judge_cdn = main_df[main_df['xpath'] == target_xpath]
    
    # 找出新a/alt結尾的xpath index
    idx_change = main_df[main_df['xpath'] == alt_xpath].index
          
    # 改為上層的xpath    
    if judge_cdn.shape[0] > 0 :
        main_df.loc[idx_change,'xpath'] = target_xpath
        
    else :
        print(f"{target_xpath} not exists")        


# ## Alter Value : Datatype為innerHTML , alt改為text

# In[19]:


main_df['datatype'] = main_df.apply(lambda x : 'text' if x['datatype'] == 'innerHTML' else ( 'text' if x['datatype'] == 'alt' else x['datatype']) , axis = 1)


# ## Join : 將Datatype為href的值以"?"符號切分

# In[20]:


split_df = main_df['value'].str.split('?' , n = 1 , expand = True)
split_df.fillna('', inplace = True)
split_df.columns = ['head_url','sub_url']
   
main_df = pd.merge(main_df , split_df , how = 'left' , left_on = main_df.index , right_on = split_df.index)
main_df.drop('value', inplace = True , axis = 1)

# split_df的head_url取代原本的value
main_df['value'] = main_df['head_url']
main_df.drop('head_url', inplace = True , axis = 1)

# drop多餘欄位
main_df['key'] = main_df['key_0']
main_df.drop('key_0', inplace = True , axis = 1)

if module == 'prd':
    del split_df


# ## Filter : 保留各"主分類"下相同value最小的key

# In[21]:


main_df['key_remain'] = main_df.groupby(['CLASS','value'])['key'].transform('min')
main_mask1 = main_df['key_remain'] == main_df['key']
main_df = main_df[main_mask1]


# ## Add : 給予group_lv2的xpath key值 (For 串接Pivot Class)

# In[22]:


main_df.sort_values('key' , inplace = True)

# 分配各"子分類"下xpath KEY值
main_df['key_gp2_xpath'] = 1
for i in range(len(main_df)):
    
    num_before = main_df.iloc[i-1]['key_gp2_xpath']
    
    if i != 0 :
        if main_df.iloc[i]['CLASS'] == main_df.iloc[i-1]['CLASS']: 
            if main_df.iloc[i]['group_lv2'] == main_df.iloc[i-1]['group_lv2']:
                
                if main_df.iloc[i]['xpath'] != main_df.iloc[i-1]['xpath'] :        
                    main_df.iloc[i,-1] = num_before +1                
                else :
                    main_df.iloc[i,-1] = num_before
                    
            else :   
                main_df.iloc[i,-1] = 1                
        else :
            main_df.iloc[i,-1] = 1


# ## Join : 取出sub_xpath為H3/H4結尾的值，並取代該Group_lv2分類

# In[23]:


# Create Table : 取出xpath為H3/H4結尾的值
new_class = main_df[main_df['sub_xpath'].str.contains('h3$|h4$')][['CLASS','value']]
new_class.columns = ['old_class','new_class']

# 合併後刪除為H3/H4結尾的該筆資料
main_df = pd.merge(main_df , new_class , left_on = 'CLASS' , right_on = 'old_class' , how = 'left' ).fillna('')
main_df.drop(main_df[main_df['sub_xpath'].str.contains('h3$|h4$')].index , inplace = True)
main_df['pivot_class'] = main_df.apply(lambda x : x['new_class'] if x['new_class'] != '' else x['CLASS']  , axis = 1)
new_class.reset_index(drop = True , inplace = True)


# ## Add : ( Pivot Class ) 經處理使得原不同Group_lv2串接後Pivot Table分類至相同

# In[24]:


for idx in main_df.index.tolist():

    if '連結' in main_df.loc[idx,'CLASS']:
        
        # H3/H4改為GroupClass名稱串上Group_lv2_key成pivot class
        if main_df.loc[idx,'new_class'] != '':   
            main_df.loc[idx,'pivot_class'] = f"{main_df.loc[idx,'new_class']}_{str(main_df.loc[idx,'key_gp2_xpath'])}"
        
        # Alter : 若CLASS為H3/H4同組，Pivot Class將更改相同名稱並保留"_key"
        else :
            c = new_class[new_class['old_class'] == main_df.loc[idx,'CLASS'][:-2]]
            old_c = c['old_class'].iloc[0]
            new_c = c['new_class'].iloc[0]
            main_df.loc[idx,'pivot_class'] = main_df.loc[idx,'CLASS'].replace(old_c,new_c)
    else :
        pass

if module == 'prd':
    del new_class
    


# ## Filter : Pivot Class下相同value保留最小key的該筆資料

# In[25]:


main_df['key_pivot_dup_value'] = main_df.groupby(['pivot_class','value'])['key'].transform('min')
main_df = main_df[main_df['key'] == main_df['key_pivot_dup_value']]


# ## Join : 將Pivot Class下datatype數量與value數量一致時，使用最小key_remain的group_lv2

# In[26]:


# mask1 : 計算pivot_class下"不重覆value數量"
main_df['cnt_pivot_value'] = main_df.groupby(['pivot_class'])['value'].transform('nunique')

# mask2 : 計算pivot_class下"Datatype幾類數量"
main_df['cnt_pivot_datatype'] = main_df.groupby(['pivot_class'])['datatype'].transform('nunique')

mask1_min_gp2_xpath = main_df['cnt_pivot_value'] == main_df['cnt_pivot_datatype']
mask2_min_gp2_xpath = main_df['cnt_pivot_value'] > 1

# Create : 挑出同pivot_class且為datatype數量與value數量一致的條件下最小key值
min_gp2_xpath = main_df[mask1_min_gp2_xpath & mask2_min_gp2_xpath][['pivot_class','group_lv2','key']].groupby('pivot_class').min().reset_index()

# 取得pivot_class下最小key值
main_df['key_min_pivot_xpath'] = main_df.groupby('pivot_class')['key'].transform('min')

# Join : min_gp2_xpath串接pivot_class最，取得group_lv2
main_df = pd.merge(main_df , min_gp2_xpath , left_on = ['pivot_class','key_min_pivot_xpath'] , right_on = ['pivot_class','key'] , how = 'left')
main_df.fillna('' , inplace = True)

# Add : 若串接min_gp_xpath的group_lv2有值，則將pivot_group_lv2設定為min_gp_xpath.group_lv2，否則main_df.group_lv2
main_df['pivot_group_lv2'] = main_df.apply(lambda x : x['group_lv2_y'] if x['group_lv2_y'] != '' else x['group_lv2_x'] , axis = 1 )


# ## Add : 在GroupBy(pivot_class , pivot_group_lv2)情況下對xpath排序取Key值 ( For Pivot Table ) 

# In[27]:


main_df['key_pivot_gp2_xpath'] = 1
for i in range(len(main_df)):
    
    num_before = main_df.iloc[i-1]['key_pivot_gp2_xpath']
    
    if i != 0 :
        if main_df.iloc[i]['pivot_group_lv2'] == main_df.iloc[i-1]['pivot_group_lv2']: 
            if main_df.iloc[i]['pivot_class'] == main_df.iloc[i-1]['pivot_class']:
                
                if main_df.iloc[i]['xpath'] != main_df.iloc[i-1]['xpath'] :        
                    main_df.iloc[i,-1] = num_before +1                
                else :
                    main_df.iloc[i,-1] = num_before
                    
            else :   
                main_df.iloc[i,-1] = 1                
        else :
            main_df.iloc[i,-1] = 1


# ## Insert DB : Main_df

# In[28]:


main_df_json = main_df.to_json(orient = 'split')
main_df_date = ETL_target_datetime
main_df_pivot = table_name
session.execute('insert into pivot_table_log (insert_time , pivot_table , detail) values (%s,%s,%s) ' , (main_df_date , main_df_pivot , main_df_json))


# In[29]:


main_df['value'] += "||"

main_pivot = main_df.pivot_table(index = ['insert_time','pivot_class','pivot_group_lv2','key_pivot_gp2_xpath'] , columns = 'datatype', values = 'value' , fill_value = '' , aggfunc = 'sum')
main_pivot.reset_index(inplace = True)
main_pivot['url'] = table_name
main_pivot.columns = ['insert_time' , 'pivot_class' , 'group_lv2' , 'key_pg2' , 'href' , 'text' , 'url' ]

#Insert : 寫入pivot_table
for _ , data in main_pivot.iterrows():
    cur.execute("insert into pivot_table (insert_time , pivot_class , group_lv2 , key_gp2 , href , text , url )                 values (%s,%s,%s,%s,%s,%s,%s)" , data)
cur.execute('commit')

if module != 'prd':
    main_pivot.to_csv(f'dev_test_files/{ETL_target_datetime}/{table_name}_main_df.csv' , index = False)
else :
    del main_pivot , main_df

cnt_main_df_in_db = pd.read_sql(f"select count(*) from pivot_table where url = '{table_name}' and insert_time = '{ETL_target_datetime}'" , con = conn)['count'][0]
print("FINAL STAGE : *{0}* {1} amounts data in Pivot_table".format(table_name,cnt_main_df_in_db))


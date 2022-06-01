#!/usr/bin/env python
# coding: utf-8

# In[1]:


import re
import pandas as pd
import datetime
import time

from selenium import webdriver
from cassandra.cluster import Cluster
from cassandra.query import dict_factory
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options
from Crawler import utils


# # Cassandra Connection

# In[2]:


cluster = Cluster(contact_points = ['172.17.0.2'] ,  port = '9042')
session = cluster.connect()
session.set_keyspace('books')
session.row_factory = dict_factory


# # Arguments

# In[3]:


with open('/home/buneoliou/Crawler/Project_Books/parameters.csv' , 'r') as f:
    param = eval(f.read())
    
table_param_dict = param['table_param_dict']
get_data_type_list = param['get_data_type_list']

home_page = 'https://www.books.com.tw/'


# # Script

# In[5]:


if __name__ == '__main__':
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    
    browser = webdriver.Chrome(options=chrome_options)
    browser.maximize_window()
    browser.get(home_page)
    
    # 確保已取得網頁完整資訊
    condition = expected_conditions.visibility_of_element_located((By.LINK_TEXT , '博客來粉絲團'))
    WebDriverWait(driver = browser , timeout = 30 , poll_frequency= 1).until(condition)
    
    print(f"Process {datetime.datetime.now().strftime('%Y%m%d%H00')} data")
    for t in range(2):    
        print('-'*30 + f'\n\033[1mRound {t+1} \033[0m\n' + '-'*30)
        html_tags , page_src = utils.Get_html_tags(browser)
        print('\033[1mFinish.\033[0m')
        
        clean_full_tags = utils.Remain_body_tags(html_tags)
        print('\033[1mFinish.\033[0m')
        
        xpath_dict , idx_dict , xpath_list = utils.Get_page_xpath(clean_full_tags,status_text = t)
        print('\033[1mFinish.\033[0m')    
        if t == 0 :
            get_data_xpath , _ = utils.Get_more_data(browser , xpath_list = xpath_list)
            print('\033[1mFinish.\033[0m')
        
        elif t == 1 :
            correct_dict , error_dict = utils.Get_attributes(browser , xpath_list , get_data_type_list)
            print('\033[1mFinish.\033[0m')
            print(f'{len(correct_dict.keys())} data amounts ')
    
    while len(correct_dict.keys()) < 7000:
        print('Data amount exceptional')
    
    utils.Insert_DB(session = session ,
              table_name = 'home_page', 
              table_param_dict = table_param_dict , 
              insert_dict = correct_dict)
    
    utils.Insert_DB(session = session ,
              table_name = 'error_xpath', 
              table_param_dict = table_param_dict , 
              insert_dict = error_dict)
    
    browser.close()


#!/usr/bin/env python
# coding: utf-8

# In[1]:


import datetime
import time
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator , BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator

from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions
from selenium.webdriver.support.wait import WebDriverWait
from selenium.webdriver.chrome.options import Options

from cassandra.cluster import Cluster
from cassandra.query import dict_factory

import pandas
import psycopg2


# # Setting

# In[2]:


start_date = Variable.get('Crawler_start_date')
start_date = datetime.datetime.strptime(start_date,'%Y%m%d %H%M%S')


# ## Arguments

# In[3]:


args = {
    'owner' : 'Buneo' ,
    'depends_on_past' : False ,
    'start_date' : start_date ,
    'email_on_failure' : 'buneostock@gmail.com' ,
    'email_on_retry' : 'buneostock@gmail.com' ,
    'retreis' : 2 ,
    'retry_delay' : datetime.timedelta(minutes = 5)
        }

dag = DAG('Crawer_Books',
          description = '' ,
          schedule_interval = '10 */2 * * *',
          default_args = args ,
          tags = ['Crawler'])


# # Script

# In[4]:


task_Homepage = BashOperator(task_id = 'task_Homepage',
                                           bash_command = 'python /home/buneoliou/Crawler/Project_Books/Script_Homepage.py' ,
                                           dag = dag ,
                                           trigger_rule = 'all_success')


# # DAG workflow

# In[5]:


task_Homepage


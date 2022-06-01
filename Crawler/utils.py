#!/usr/bin/env python
# coding: utf-8

# # Using Package

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


# # Get_html_tags

# In[2]:


def Get_html_tags(browser):
    '''Remain significant tags (not contains annotated tags)'''
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Get_html_tags']['status_text'])
        
    tag_pattern = r'<\w+[a-z\s>]|</[\w+]+?>'
    
    page_src = browser.page_source
    ignore_tag = re.findall('(\<\!\-\-.*\-*\-*\>*)(\s*)(.*\-\-\>)' , page_src)
    
    while ignore_tag != []:
        page_src = page_src.replace(ignore_tag[0][0],'')
        page_src = page_src.replace(ignore_tag[0][2],'')
        ignore_tag = re.findall('(\<\!\-\-.*\-*\-*\>*)(\s*)(.*\-\-\>)' , page_src)
        
    page_full_tags = re.findall(tag_pattern,page_src)

    return page_full_tags , page_src


# # Remain_body_tags

# In[3]:


def Remain_body_tags(page_full_tag , print_out = False):
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Remain_body_tags']['status_text'])
    
    body_tag = []
    clean_body_tag = []
    pop_list = ['link','br','pre','hr','b','i','u','sup','sub','font','tr','th','td','bgsound','embed','strong','em',
            'map','script','noscript','time','blockquote','area','button','input']
    
    try :
        head_body_index = page_full_tag.index('<body>')
    except ValueError:
        head_body_index = page_full_tag.index('<body ')
    
    try:
        end_body_index = page_full_tag.index('</body>')
    except ValueError:
        end_body_index = page_full_tag.index('</body ')
        
    body_tag = page_full_tag[head_body_index+1:end_body_index]
    
    for tag in body_tag:
        clean_body_tag.append(tag.replace('<','').replace('>','').replace(' ',''))
        

    for target in pop_list:

        h = 0
        t = 0
        try:
            while clean_body_tag.index(target) or clean_body_tag.index(target) == 0 :
                clean_body_tag.pop(clean_body_tag.index(target))
                h = h + 1
        except ValueError:
            pass
            
        try:
            while clean_body_tag.index(f"/{target}") or clean_body_tag.index(f"/{target}") == 0:
                clean_body_tag.pop(clean_body_tag.index(f"/{target}"))
                t = t + 1
        except ValueError:
            pass
        if print_out :
            print('【 {0:<10s} 】 Head : {1} ; Tail {2} \r\n----------------------'.format(target,h,t))      
    return clean_body_tag


# # Get_index

# In[4]:


def Get_index(xpath , idx_dict ):
    try : 
        match = re.match('(.*/)(\w+)(\[*\d*\]*)',xpath)
        pre_xpath = match.group(1)+match.group(2)
        
        if idx_dict.get(pre_xpath) is None:
            idx_dict[pre_xpath] = 1
        else :
            idx_dict[pre_xpath] += 1
            
        xpath = f'{pre_xpath}[{idx_dict[pre_xpath]}]'
    except :
        if idx_dict.get(xpath) is None:
            idx_dict[xpath] = 1
        else :
            idx_dict[xpath] += 1
            
        xpath = f'{xpath}[{idx_dict[xpath]}]'
    return xpath , idx_dict


# # Determine_xpath_full_end

# In[5]:


def Determine_xpath_full_end(xpath , i , xpath_dict , idx_dict , print_out = False):
    next_div_idx = ''
    head_xpath , tail_xpath = xpath.split('|')[0] , xpath.split('|')[1]
    if head_xpath.split('/') == tail_xpath.split('/')[::-1]:
        xpath_dict[i] = {}
        xpath_dict[i]['head_xpath'] = head_xpath
        xpath_dict[i]['tail_xpath'] = tail_xpath
        xpath_dict[i]['final_tag'] = head_xpath.split('/')[-1]
        if print_out :
            print('xpath_Dict append:',head_xpath)
        xpath = ''  
        _ , idx_dict = Get_index(head_xpath.split('/')[0] , idx_dict)
        
    return xpath , xpath_dict , idx_dict


# # Get_xpath_group

# In[6]:


def Get_xpath_group(xpath):

    try :

        div_lv1 = re.findall('(.*?)(/a|/form|/h2|/h3|/h4|/img|/li|/p|/span|/ul)', xpath)[0][0]
       
        div_lv2_findall = re.findall('(/.*)(div\[*\d*\]*)(/a|/form|/h2|/h3|/h4|/img|/li|/p|/span|/ul|/)'
                                        ,'/'+ xpath[len(div_lv1)+1:] +'/')
        if div_lv2_findall != []:  
            div_lv2 = div_lv1 + div_lv2_findall[0][0] + div_lv2_findall[0][1]
            sub_xpath = xpath[len(div_lv2):]
            
        else :
            div_lv2 = ''
            sub_xpath = xpath[len(div_lv1)+1:]
    except :
        div_lv1 = xpath
        div_lv2 = ''
        sub_xpath = ''
    
    return div_lv1 , div_lv2 , sub_xpath


# # Get_page_xpath

# In[25]:


def Get_page_xpath(clean_body_tag , status_text:int , print_out = False):
    
    '''cant contain below tags\r\n'link','br','p','pre','hr','b','i','u','sup','sub','font','tr','th','td','bgsound','embed','strong','em',
            'map','script','noscript','time','blockquote','area','button','input' '''
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Get_page_xpath']['status_text'][status_text])
    
    xpath_dict = {}
    xpath = ''
    prev_xpath = ''
    prev_xpath_idx = 0
    next_head_div_idx = ''
    idx_dict = {}
    
    for i , tag in enumerate(clean_body_tag):
        
        # 情境一：Xpath起頭
        if xpath == '' :
                                   
            xpath , idx_dict = Get_index(tag , idx_dict)
            
            if tag == 'img':
                xpath += '|img'
                xpath , xpath_dict , idx_dict = Determine_xpath_full_end(xpath , i , xpath_dict , idx_dict , print_out)
            
            if print_out :    
                print("\033[1m{0}\033[0m Head Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))
        
        # 情境二：TAG為Xpath結尾標籤
        elif tag[0] == '/':
            
            if xpath.find('|') > 0:
                
                xpath += '/' + tag[1:]                
                # 前有多個結尾標籤，若增加此TAG後為完整Xpath即寫入Dict
                xpath , xpath_dict , idx_dict = Determine_xpath_full_end(xpath , i , xpath_dict , idx_dict , print_out)
                     
                if xpath != '':
                    if print_out:
                        print("\033[1m{0}\033[0m Add End Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))
    
            else : 
                
                xpath += '|' + tag[1:]     
                
                # 單一結尾標籤即為完整Xpath即寫入Dict
                xpath , xpath_dict , idx_dict = Determine_xpath_full_end(xpath , i , xpath_dict , idx_dict , print_out)
                
                if xpath != '':
                    if print_out:
                        print("\033[1m{0}\033[0m Add End Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))
        
        # 情境三：此次為開頭TAG , 此TAG前的TAG皆為結尾TAG , 因此判斷為完整區段Xpath , 換下一區段Xpath
        else :
            
            if xpath.find('|') > 0:
                    
                # 進行比對找出結尾TAG , 保留最後結尾TAG及TAG索引數去判斷下個開頭Xpath是否需增加TAG索引數
                head_xpath , tail_xpath = xpath.split('|')[0] , xpath.split('|')[1]
                final_tag = tail_xpath.split('/')[-1]
                                    
                # 迴圈找出最後結尾段 , 保留下一段的相同段
                idx = 0                    
                while idx < len(tail_xpath.split('/')):
                    if re.match('(\w+)',head_xpath.split('/')[-idx-1]).group(0) ==                        re.match('(\w+)',tail_xpath.split('/')[idx]).group(0):
                        remain_xpath = head_xpath.split('/')[:-idx-1]                        
                    idx = idx + 1
              
                   
                # 紀錄完整Xpath及結尾段xpath
                xpath_dict[i] = {}
                xpath_dict[i]['head_xpath'] = head_xpath
                xpath_dict[i]['tail_xpath'] = tail_xpath
                xpath_dict[i]['final_tag'] = head_xpath.split('/')[-1]
                
                if print_out:
                    print('-'*40 + f'\r\n\033[1mAppend\033[0m Section Xpath to Dict :【 {head_xpath} 】')
                                
                xpath = ''
                
                # 取出下一段xpath相同段
                for idx , path in enumerate(remain_xpath,1):
                    if idx == 1:
                        xpath = path
                    else :
                        xpath += '/' + path                         
                    
                                
                if xpath == '':
  
                    xpath , idx_dict = Get_index(tag,idx_dict )
                    if tag == 'img':
                        xpath += '|img'
                    #xpath = '/' + xpath
                        
                    if print_out:
                        
                        print(f'\033[1mNext\033[0m Head of Section Xpath : 【 {xpath} 】\r\n'+'-'*40)        
                        print("\033[1m{0}\033[0m Head Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))                    
                        
                    
                else:
                    xpath += '/' + tag
                    xpath , idx_dict = Get_index(xpath , idx_dict )
                    if print_out:
                        
                        print(f'\033[1mNext\033[0m Head of Section Xpath : 【 {xpath} 】\r\n'+'-'*40)
                        print("\033[1m{0}\033[0m Add Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))
            
            else: 
                xpath += '/' + tag
                xpath , idx_dict = Get_index(xpath , idx_dict)
                if tag == 'img':
                    xpath += '|img'
                
                if print_out:
                    print("\033[1m{0}\033[0m Add Tag:【 {1:<8s}】 ,\tFull Xpath : {2}".format(i,tag,xpath))
     
    xpath_list = []
    for key in list(xpath_dict.keys()):
        xpath_list.append(xpath_dict[key]['head_xpath'].replace('[1]',''))

    return xpath_dict , idx_dict , xpath_list


# # Get_more_data_for_homepage

# In[12]:


def Get_more_data_for_homepage(browser , xpath_list):
    s = 'div[3]/div/div[6]/div/div/div[2]/ul'
    for xpath in xpath_list :
        if s in xpath:
            try:
                e = browser.find_element(by = 'xpath' , value = f'/html/body/{xpath}')
                ActionChains(browser).move_to_element(e).perform()
                time.sleep(0.5)       
            except Error as e:
                pass


# # Get_more_data

# In[13]:


def Get_more_data(browser , xpath_list):
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Get_more_data']['status_text'])
    
    
    get_data_xpath = []
    get_data_error_xpath = []
    
    for xpath in xpath_list:
        final_tag = xpath.split('/')[-1]
        if re.match('(\w+)(\[*\d*\]*)',final_tag).group(1) in ['div','li','ui','span']:
            get_data_xpath.append(xpath)
            
    for xpath in get_data_xpath:
        try :
            e = browser.find_element(by = 'xpath' , value = f"/html/body/{xpath}")
            ActionChains(browser).move_to_element(e).perform()
            #ActionChains(browser).click(e).perform()
            time.sleep(0.5)
        except :
            get_data_error_xpath.append(xpath)
        
    Get_more_data_for_homepage(browser , xpath_list)
    
    return get_data_xpath , get_data_error_xpath


# # Get_attributes

# In[27]:


def Get_attributes(browser , xpath_list , get_data_type_list):
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Get_attributes']['status_text'])
        
    correct_dict = {}
    error_dict = {}
    check_list = []
    duplicate_list = []
    
    Is_img_idx = lambda x : 'p' if x == 'img' else ('s' if x == 'span' else '')
    
    for i , tmp_xpath in enumerate(xpath_list,1):
        xpaths = Get_expand_xpath(tmp_xpath)
        
        for i_x , xpath in enumerate(xpaths):            
            
            try :                
                r = browser.find_element(by = 'xpath' , value = '/html/body/' + xpath )
                data_type = re.match('(\w+)' , xpath.split('/')[-1]).group()
                target_attr = get_data_type_list[data_type]
                
                if xpath not in check_list :
                    check_list.append(xpath)
                    
                else :
                    if len(xpaths) > 1 and data_type == 'a':
                        
                        for _ in range(len(target_attr)-1):
                            duplicate_list.append(xpath)
                    else :        
                        for _  in target_attr :                        
                            duplicate_list.append(xpath)
                
                for attr_i , _ in enumerate(target_attr):                                
                     
                    attr = target_attr[attr_i]
                    
                    if len(xpaths) > 1 and data_type == 'a' and attr == 'innerHTML':
                        pass
                    else :
                        correct_dict[f"{i}-{i_x}-{attr_i+1}"] = {}
                        correct_dict[f"{i}-{i_x}-{attr_i+1}"]['xpath'] = xpath
                        correct_dict[f"{i}-{i_x}-{attr_i+1}"]['datatype'] = attr
                        correct_dict[f"{i}-{i_x}-{attr_i+1}"]['final_tag'] = data_type
                        if attr == 'text' :
                            correct_dict[f"{i}-{i_x}-{attr_i+1}"]['value'] = r.text
                            
                        else :
                            correct_dict[f"{i}-{i_x}-{attr_i+1}"]['value'] = r.get_attribute(f"{attr}")
                                         
            except :
                error_dict[i] = {}
                error_dict[i]['xpath'] = tmp_xpath
    
    insert_time = datetime.datetime.now().strftime('%Y%m%d%H00')
    
    for k in correct_dict.keys():
        group_lv1 , group_lv2 , sub_xpath = Get_xpath_group(correct_dict[k]['xpath'])
        correct_dict[k]['group_lv1'] = group_lv1
        correct_dict[k]['group_lv2'] = group_lv2
        correct_dict[k]['sub_xpath'] = sub_xpath
        correct_dict[k]['insert_time'] = insert_time
    
    current_url = browser.current_url
    
    for k in error_dict.keys():
        error_dict[k]['insert_time'] = insert_time
        error_dict[k]['url'] = current_url
        
    print(f'{len(duplicate_list)} duplicate datas')

    return correct_dict , error_dict 


# # Get_expand_xpath

# In[15]:


def Get_expand_xpath(xpath):
    
    expand_condition = [['a','img'],['a','span']]
    
    pure_xpath = []
    expand_xpath = [xpath]
    
    xpath_split = xpath.split('/')
    for tag in xpath_split:
        pure_xpath.append(re.match('\w+',tag).group())
    
    if len(pure_xpath) >2 :
        if [pure_xpath[-2],pure_xpath[-1]] in expand_condition :
            expand_xpath.append(xpath[:-len(xpath_split[-1])-1])
            
    return expand_xpath


# # Insert_DB

# In[16]:


def Insert_DB(session , table_name , table_param_dict , insert_dict : dict):
    
    with open('/home/buneoliou/Crawler/Project_Books/parameters.csv','r') as f:
        print(eval(f.read())['function_text']['Insert_DB'][table_name]['status_text'])
        
    columns = table_param_dict[table_name]['columns']    
    values_cql = lambda x : 'values (%s' + ',%s'*(len(x.split(','))-1) + ')' if len(x.split(',')) > 1 else 'values (%s)'
    insert_cql = f'INSERT INTO {table_name} ({columns}) {values_cql(columns)}'
    
    for k in insert_dict.keys():        
        #print([ indsert_dict[k][c] for c in columns.replace(' ','').split(',') ])
        session.execute(insert_cql , [ insert_dict[k][c] for c in columns.replace(' ','').split(',') ])
        
    cql_amt = session.execute("select count(*) from {0} where insert_time = '{1}' allow filtering".format(table_name , insert_dict[k]['insert_time'] )).one()['count']
    print('Home page {0} Data Inserted {1} amounts'.format(insert_dict[k]['insert_time'] , cql_amt))


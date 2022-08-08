#!/usr/bin/env python
# coding: utf-8

# In[5]:


import pandas as pd


# In[12]:


def insert_unmatched_log(df , con , table_name ):
    cur = con.cursor()    
    unmatched_df = df[df['CLASS'] == '']
    
    if unmatched_df.shape[0] != 0:
        df.drop(unmatched_df.index , inplace = True)
        unmatched_df['url'] = table_name
        insert_time = unmatched_df.iloc[0]['insert_time']
        unmatched_df = unmatched_df[['url','insert_time','group_lv1','group_lv2','xpath']]
        
        for _ , data in unmatched_df.iterrows():
            cur.execute("insert into unmatched_log (url , insert_time , group_lv1 , group_lv2 , xpath) values (%s,%s,%s,%s,%s)",                       data)
            cur.execute("commit")
            
        amt_unmatched = pd.read_sql(f"select count(*) from unmatched_log where url = '{table_name}' and insert_time = '{insert_time}'" , con = con)['count'][0]
        print(f"unmatched_log :{table_name}_{insert_time} inserted {amt_unmatched} amounts data into ")
    
    else :
        print("unmatched_log : No data inserted")
    
    return df


# In[13]:


def insert_ignore_log(df , con , table_name ):
    
    cur = con.cursor()    
    Ignore_df = df[df['CLASS'] == 'Ignore']
    df.drop(Ignore_df.index , inplace = True)
    Ignore_df.loc[:,'url'] = table_name
    Ignore_df = Ignore_df[['url','insert_time','group_lv1','group_lv2','xpath','value']]
    insert_time = Ignore_df.iloc[0]['insert_time']    
    
    for _ , data in Ignore_df.iterrows():
        
        i = 0 
        
        try :
            cur.execute("insert into ignore_log (url , insert_time , group_lv1 , group_lv2 , xpath , value ) values (%s,%s,%s,%s,%s,%s)" , data)
            i += 1
            
        except :
            pass
        
    cur.execute("commit")
    
    if i > 0 :
        print(f"ignore_log : {table_name} inserted {i} amounts ")
    else :
        print("ignore_log : No data inserted")

    return df
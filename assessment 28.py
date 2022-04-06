#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd 


# In[3]:


import requests


# In[4]:


from bs4 import BeautifulSoup


# In[15]:


import json
from sqlalchemy import create_engine


# In[16]:


# get the response in the form of html
wikiurl="https://en.wikipedia.org/wiki/List_of_prime_ministers_of_India"
table_class="wikitable sortable jquery-tablesorter"
response=requests.get(wikiurl)
print(response.status_code)


# In[17]:


# parse data from the html into a beautifulsoup object
soup = BeautifulSoup(response.text, 'html.parser')
indiatable=soup.find('table',{'class':"wikitable"})


# In[18]:


print(indiatable)


# In[19]:


fd=pd.read_html(str(indiatable))
# convert list to dataframe
fd=pd.DataFrame(fd[0])
print(fd.head())


# In[20]:


fd


# In[10]:


pip install sqlalchemy


# In[21]:


my_conn=create_engine("mysql+pymysql://root:prabhakar@localhost/db2")


# In[14]:


from sqlachemy import create_engine


# In[26]:





# In[27]:


gd


# In[29]:


fg=fd


# In[30]:


fg.colums = ['(BJP (2)[b] INC/INC(I)/INC(R) [c] (6+1 acting[d]) JD (3) JP (1) JP(S) (1) SJP(R) (1), No, No)', 'column_Name_2'] method.fg


# In[32]:


fg.to_csv("india.csv")


# In[42]:


fg.columns=["No","No.1","Portrait","Name","Party","Took office","Left office","Time in office","Lok Sabha[e]","Ministry","Appointed by","Not needed"]
fgg=fg[["Name","Party","Took office","Left office"]]


# In[43]:


fgg


# In[44]:


fgg.drop(26)


# In[45]:


my_conn=create_engine("mysql+pymysql://root:prabhakar@localhost/db2")


# In[46]:


fgg.to_sql("india_prime",my_conn,if_exists='replace',index=False)


# In[ ]:





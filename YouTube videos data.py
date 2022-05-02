#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests


# In[205]:


import json


# In[206]:


import pandas as pd


# In[207]:


from bs4 import BeautifulSoup


# In[218]:


url= "https://www.googleapis.com/youtube/v3/videos?id=6AhvyTENViU&key=AIzaSyDt5RCukN1plquKk0N8bw7d4MuV0_-3_Wo&part=snippet,contentDetails,statistics,status"


# In[219]:


page=requests.get(url)


# In[220]:


json_struc=page.json()


# In[221]:


df = pd.json_normalize(json_struc['items'])


# In[222]:


df


# In[223]:


df1= df[df.columns[df.columns.isin(['id','snippet.title','snippet.description','snippet.publishedAt','statistics.viewCount','statistics.likeCount','statistics.favoriteCount','statistics.commentCount','snippet.categoryId','snippet.tags','snippet.channelId', 'snippet.channelTitle'])]]
df1


# In[224]:


df1


# In[225]:


df1.columns=['ID','PUBLISHEDAT','CHANNELID','TITLE', 'DESCRIPTION','CHANNELTITLE','TAGS','CATEGORYID','VIEWCOUNT','LIKECOUNT','FAVORITECOUNT','COMMENTCOUNT']
df1


# In[226]:


from sqlalchemy import create_engine


# In[232]:


import pymysql


# In[239]:


import os


# In[238]:


df1.to_csv('/Users/prabhakarbalaram/Downloads/youtube.csv')


# In[240]:


os.makedirs('/Users/prabhakarbalaram/Downloads',exist_ok=True)
df1.to_csv('/Users/prabhakarbalaram/Downloads/youtube.csv',index=False)


# In[252]:


import pandas as pd
import pandasql as ps
rav=pd.read_csv('/Users/prabhakarbalaram/Downloads/youtube.csv')
rav.columns=['ID','PUBLISHEDAT','CHANNELID','TITLE', 'DESCRIPTION','CHANNELTITLE','TAGS','CATEGORYID','VIEWCOUNT','LIKECOUNT','FAVORITECOUNT','COMMENTCOUNT']


# In[253]:


rav


# In[254]:


connection= pymysql.connect(host='localhost',
user ='root',
password='prabhakar',
db='db2')

cursor=connection.cursor()


# In[257]:


cols = "`,`".join([str(i) for i in rav.columns.tolist()])



# Insert DataFrame recrds one by one.
for i,row in rav.iterrows():
    sql = "INSERT INTO `YTUBE_VIDEOS` (`" +cols + "`) VALUES (" + "%s,"*(len(row)-1) + "%s)"
    cursor.execute(sql, tuple(row))



# the connection is not autocommitted by default, so we must commit to save our changes
connection.commit()


# In[258]:


cols="`,`".join([str(i) for i in rav.columns.tolist()])
#insert Dataframe records one by one
for  i ,row in rav.iterrows():
    sql="INSERT INTO `YTUBE_VIDEOS` (`" +cols + "`)VALUES (" + " %s,"*(len(row)-1) + "%s)"
    cursor.execute(sql,tuple(row))
    
connection.commit()


# In[ ]:





# In[ ]:





# In[ ]:





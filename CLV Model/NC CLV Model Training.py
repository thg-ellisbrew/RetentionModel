#!/usr/bin/env python
# coding: utf-8
import CsPy_Uploading as cs
# In[1]:


import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from datetime import datetime
import os


# In[2]:


query = """

            SELECT * FROM `0_Ellis_B.NC_CLV_Model_Training_Data_*`
            
            WHERE 1=1
                AND _TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -540 DAY) AS STRING), '-','') AND REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -181 DAY) AS STRING), '-','')

"""


data = pd.DataFrame(cs.DownloadJob(
      query = query
    , input_data_from='BQ'
    , output_data_type='DATAFRAME',
    # data_file='',
    # dataframe='',
    # columns='',
    # upload_data_type='',
    # bq_key_path='',
    # bq_key_name='',
    # bq_upload_type='',
    # sql_server='',
    # sql_key_path='',
    # sql_key_name='',
    save_file_path=os.path.join(os.path.dirname(__file__), 'CSV/'),
    account_first_name='Ellis',
    account_surname='Brew',
    # account_file_path='',
    # set_logging=True,
    # set_testing=False,
    # set_date_conversion=True
    set_open_file=False,
    set_clear_save_file_location=False).run())

data['order_date'] = pd.to_datetime(data['order_date'])
data['revenue'] = np.round(data['revenue'],2)
data = data[data['revenue'] > 0]


# In[3]:


max_time = data['order_date'].max()
data['recency_index'] = (max_time - data['order_date']).dt.days


# In[4]:


for month in range(1, 13):
    data[f'month_{month}'] = data['order_date'].dt.month == month

# Convert boolean columns to integers (0 or 1)
data = data.astype({f'month_{month}': 'int' for month in range(1, 13)})


# In[5]:


data = pd.get_dummies(data, columns=['device'], drop_first=True)
data = pd.get_dummies(data, columns=['Channel'], drop_first=True)


# In[6]:


## log transforms

data['CLV_180'] = np.log1p(data['CLV_180'])
data['units'] = np.log1p(data['units'])
data['revenue'] = np.log1p(data['revenue'])
data['myprotein_rev'] = np.log1p(data['myprotein_rev'])
data['bfsd_rev'] = np.log1p(data['bfsd_rev'])
data['vit_rev'] = np.log1p(data['vit_rev'])
data['clothing_rev'] = np.log1p(data['clothing_rev'])
data['percentage_discount'] = np.log1p(data['percentage_discount'])


# In[7]:


data['units * discount'] = np.log1p(data['units'] * data['percentage_discount'])


# In[8]:


GB = data[data['locale_key']==3]
DE = data[data['locale_key']==2]
US = data[data['locale_key']==12]
JP = data[data['locale_key']==13]


# In[9]:


GB['discount vs avg'] = GB['percentage_discount'] - GB['percentage_discount'].mean()
DE['discount vs avg'] = DE['percentage_discount'] - DE['percentage_discount'].mean()
US['discount vs avg'] = US['percentage_discount'] - US['percentage_discount'].mean()
JP['discount vs avg'] = JP['percentage_discount'] - JP['percentage_discount'].mean()


# In[10]:


GB = GB[GB['CLV_180'] < (GB['CLV_180'].mean() + (3*np.std(GB['CLV_180'])))]
DE = DE[DE['CLV_180'] < (DE['CLV_180'].mean() + (3*np.std(DE['CLV_180'])))]
US = US[US['CLV_180'] < (US['CLV_180'].mean() + (3*np.std(US['CLV_180'])))]
JP = JP[JP['CLV_180'] < (JP['CLV_180'].mean() + (3*np.std(JP['CLV_180'])))]


# In[11]:


## FLASH SALE FEATURE FOR JAPAN

JP['flash'] = (JP['order_date'].dt.month == JP['order_date'].dt.day).astype(int)


# In[12]:


JP_retained = JP[JP['Retention_180'] == 1]
JP_oversampled_rows = JP_retained.sample(n=len(JP_retained)*2, replace=True, random_state=42)
JP = pd.concat([JP, JP_oversampled_rows], ignore_index=True)

GB_retained = GB[GB['Retention_180'] == 1]
GB_oversampled_rows = GB_retained.sample(n=len(GB_retained)*2, replace=True, random_state=42)
GB = pd.concat([GB, GB_oversampled_rows], ignore_index=True)

DE_retained = DE[DE['Retention_180'] == 1]
DE_oversampled_rows = DE_retained.sample(n=len(DE_retained)*2, replace=True, random_state=42)
DE = pd.concat([DE, DE_oversampled_rows], ignore_index=True)

US_retained = US[US['Retention_180'] == 1]
US_oversampled_rows = US_retained.sample(n=len(US_retained)*2, replace=True, random_state=42)
US = pd.concat([US, US_oversampled_rows], ignore_index=True)


# In[13]:


X_drops = ['locale_key', 'CLV_180', 'Retention_180', 'order_date']


# # CLV Model

# In[14]:


GB_train, GB_test, GB_y_train, GB_y_test = train_test_split(GB.drop(X_drops, axis=1), GB['CLV_180'], test_size=0.3, random_state=42)
DE_train, DE_test, DE_y_train, DE_y_test = train_test_split(DE.drop(X_drops, axis=1), DE['CLV_180'], test_size=0.3, random_state=42)
US_train, US_test, US_y_train, US_y_test = train_test_split(US.drop(X_drops, axis=1), US['CLV_180'], test_size=0.3, random_state=42)
JP_train, JP_test, JP_y_train, JP_y_test = train_test_split(JP.drop(X_drops, axis=1), JP['CLV_180'], test_size=0.3, random_state=42)


# In[15]:


scaler = StandardScaler()

# List of numerical columns to scale
numerical_columns = ['units', 'percentage_discount', 'AUV', 'revenue', 'myprotein_rev', 
                     'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev', 
                     'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units']

# Fit the scaler on the training data, then transform both train and test sets
GB_train[numerical_columns] = scaler.fit_transform(GB_train[numerical_columns])
DE_train[numerical_columns] = scaler.fit_transform(DE_train[numerical_columns])
US_train[numerical_columns] = scaler.fit_transform(US_train[numerical_columns])
JP_train[numerical_columns] = scaler.fit_transform(JP_train[numerical_columns])

GB_test[numerical_columns] = scaler.transform(GB_test[numerical_columns])
DE_test[numerical_columns] = scaler.transform(DE_test[numerical_columns])
US_test[numerical_columns] = scaler.transform(US_test[numerical_columns])
JP_test[numerical_columns] = scaler.transform(JP_test[numerical_columns])


# In[16]:


GB_CLV = xgb.XGBRegressor()
DE_CLV = xgb.XGBRegressor()
US_CLV = xgb.XGBRegressor()
JP_CLV = xgb.XGBRegressor()


# In[17]:


GB_CLV.fit(GB_train, GB_y_train)
DE_CLV.fit(DE_train, DE_y_train)
US_CLV.fit(US_train, US_y_train)
JP_CLV.fit(JP_train, JP_y_train)


# In[18]:


import pickle



with open(r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Model\GB_CLV.pkl', 'wb') as file:
    pickle.dump(GB_CLV, file)

with open(r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Model\DE_CLV.pkl', 'wb') as file:
    pickle.dump(DE_CLV, file)

with open(r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Model\JP_CLV.pkl', 'wb') as file:
    pickle.dump(JP_CLV, file)

with open(r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Model\US_CLV.pkl', 'wb') as file:
    pickle.dump(US_CLV, file)


# In[ ]:





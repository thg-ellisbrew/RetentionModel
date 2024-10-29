import pandas as pd
import numpy as np
import xgboost as xgb
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from datetime import datetime


data = pd.read_csv(r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Orders to Predict.csv')
data = data.drop('Unnamed: 0', axis=1)
data['order_date'] = pd.to_datetime(data['order_date'], dayfirst=True)
data['revenue'] = np.round(data['revenue'],2)


max_time = data['order_date'].max()
data['recency_index'] = (max_time - data['order_date']).dt.days

channels = ['internal referral', 'ppc', 'influencer', 'direct',
            'messaging app', 'mobile app', 'organic', 'email', 'paid social',
            'push notification', 'other', 'not tracked', 'referral', 'social',
            'sms', 'referral scheme', 'display']

for channel in channels:
    data[f'Channel_{channel}'] = data['Channel'] == channel

data = data.astype({f'Channel_{channel}': 'int' for channel in channels})

data = data.drop('Channel', axis=1)


for month in range(1, 13):
    data[f'month_{month}'] = data['order_date'].dt.month == month

# Convert boolean columns to integers (0 or 1)
data = data.astype({f'month_{month}': 'int' for month in range(1, 13)})



device_dummies = pd.get_dummies(data, columns=['device'], drop_first=True)
data = pd.get_dummies(data, columns=['device'], drop_first=True)


## log transforms
data['units'] = np.log1p(data['units'])
data['revenue'] = np.log1p(data['revenue'])
data['myprotein_rev'] = np.log1p(data['myprotein_rev'])
data['bfsd_rev'] = np.log1p(data['bfsd_rev'])
data['vit_rev'] = np.log1p(data['vit_rev'])
data['clothing_rev'] = np.log1p(data['clothing_rev'])
data['percentage_discount'] = np.log1p(data['percentage_discount'])


data['units * discount'] = np.log1p(data['units'] * data['percentage_discount'])

GB = data[data['locale_key']==3]
DE = data[data['locale_key']==2]
US = data[data['locale_key']==12]
JP = data[data['locale_key']==13]

GB['discount vs avg'] = GB['percentage_discount'] - GB['percentage_discount'].mean()
DE['discount vs avg'] = DE['percentage_discount'] - DE['percentage_discount'].mean()
US['discount vs avg'] = US['percentage_discount'] - US['percentage_discount'].mean()
JP['discount vs avg'] = JP['percentage_discount'] - JP['percentage_discount'].mean()

## FLASH SALE FEATURE FOR JAPAN

JP['flash'] = (JP['order_date'].dt.month == JP['order_date'].dt.day).astype(int)

X_drops = ['locale_key', 'CLV_180', 'Retention_180', 'order_date']

import pickle


with open(r'CLV Model\GB_CLV.pkl', 'rb') as file:
    GB_CLV = pickle.load(file)

with open(r'CLV Model\DE_CLV.pkl', 'rb') as file:
    DE_CLV = pickle.load(file)

with open(r'CLV Model\JP_CLV.pkl', 'rb') as file:
    JP_CLV = pickle.load(file)

with open(r'CLV Model\US_CLV.pkl', 'rb') as file:
    US_CLV = pickle.load(file)

scaler = StandardScaler()

# List of numerical columns to scale
numerical_columns = ['units', 'percentage_discount', 'AUV', 'revenue', 'myprotein_rev',
                     'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev',
                     'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units']

# Fit the scaler on the training data, then transform both train and test sets
GB[numerical_columns] = scaler.fit_transform(GB[numerical_columns])
DE[numerical_columns] = scaler.fit_transform(DE[numerical_columns])
US[numerical_columns] = scaler.fit_transform(US[numerical_columns])
JP[numerical_columns] = scaler.fit_transform(JP[numerical_columns])


GB = GB[['order_number', 'locale_key', 'order_date', 'NC', 'month', 'Quarter', 'Day', 'Week', 'units', 'percentage_discount', 'AUV', 'revenue', 'RRP', 'myprotein_rev', 'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev', 'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units', 'GWP_Value', 'recency_index', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'device_mobile', 'device_tablet', 'Channel_direct', 'Channel_display', 'Channel_email', 'Channel_influencer', 'Channel_internal referral', 'Channel_messaging app', 'Channel_mobile app', 'Channel_not tracked', 'Channel_organic', 'Channel_other', 'Channel_paid social', 'Channel_ppc', 'Channel_push notification', 'Channel_referral', 'Channel_referral scheme', 'Channel_sms', 'Channel_social', 'units * discount', 'discount vs avg']]
DE = DE[['order_number', 'locale_key', 'order_date', 'NC', 'month', 'Quarter', 'Day', 'Week', 'units', 'percentage_discount', 'AUV', 'revenue', 'RRP', 'myprotein_rev', 'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev', 'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units', 'GWP_Value', 'recency_index', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'device_mobile', 'device_tablet', 'Channel_direct', 'Channel_display', 'Channel_email', 'Channel_influencer', 'Channel_internal referral', 'Channel_messaging app', 'Channel_mobile app', 'Channel_not tracked', 'Channel_organic', 'Channel_other', 'Channel_paid social', 'Channel_ppc', 'Channel_push notification', 'Channel_referral', 'Channel_referral scheme', 'Channel_sms', 'Channel_social', 'units * discount', 'discount vs avg']]
JP = JP[['order_number', 'locale_key', 'order_date', 'NC', 'month', 'Quarter', 'Day', 'Week', 'units', 'percentage_discount', 'AUV', 'revenue', 'RRP', 'myprotein_rev', 'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev', 'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units', 'GWP_Value', 'recency_index', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'device_mobile', 'device_tablet', 'Channel_direct', 'Channel_display', 'Channel_email', 'Channel_influencer', 'Channel_internal referral', 'Channel_messaging app', 'Channel_mobile app', 'Channel_not tracked', 'Channel_organic', 'Channel_other', 'Channel_paid social', 'Channel_ppc', 'Channel_push notification', 'Channel_referral', 'Channel_referral scheme', 'Channel_sms', 'Channel_social', 'units * discount', 'discount vs avg', 'flash']]
US = US[['order_number', 'locale_key', 'order_date', 'NC', 'month', 'Quarter', 'Day', 'Week', 'units', 'percentage_discount', 'AUV', 'revenue', 'RRP', 'myprotein_rev', 'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev', 'veg_rev', 'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units', 'GWP_Value', 'recency_index', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5', 'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11', 'month_12', 'device_mobile', 'device_tablet', 'Channel_direct', 'Channel_display', 'Channel_email', 'Channel_influencer', 'Channel_internal referral', 'Channel_messaging app', 'Channel_mobile app', 'Channel_not tracked', 'Channel_organic', 'Channel_other', 'Channel_paid social', 'Channel_ppc', 'Channel_push notification', 'Channel_referral', 'Channel_referral scheme', 'Channel_sms', 'Channel_social', 'units * discount', 'discount vs avg']]

GB['CLV_Prediction'] = np.exp(GB_CLV.predict(GB.drop(['order_date', 'locale_key', 'order_number'], axis=1)))
DE['CLV_Prediction'] = np.exp(DE_CLV.predict(DE.drop(['order_date', 'locale_key', 'order_number'], axis=1)))
US['CLV_Prediction'] = np.exp(US_CLV.predict(US.drop(['order_date', 'locale_key', 'order_number'], axis=1)))
JP['CLV_Prediction'] = np.exp(JP_CLV.predict(JP.drop(['order_date', 'locale_key', 'order_number'], axis=1)))

output = pd.concat([GB,DE,US,JP], ignore_index=True)


dummy_columns = output.filter(like="Channel_")
original_category = dummy_columns.idxmax(axis=1).str.replace("Channel_", "", regex=False)

# Add it back to the DataFrame
output["Channel"] = original_category

dummy_columns = output.filter(like="device_")
original_category = dummy_columns.idxmax(axis=1).str.replace("device_", "", regex=False)

# Add it back to the DataFrame
output["Device"] = original_category


output = output.drop(['recency_index', 'month_1', 'month_2', 'month_3', 'month_4', 'month_5',
       'month_6', 'month_7', 'month_8', 'month_9', 'month_10', 'month_11',
       'month_12', 'device_mobile', 'device_tablet', 'Channel_direct',
       'Channel_display', 'Channel_email', 'Channel_influencer',
       'Channel_internal referral', 'Channel_messaging app',
       'Channel_mobile app', 'Channel_not tracked', 'Channel_organic',
       'Channel_other', 'Channel_paid social', 'Channel_ppc',
       'Channel_push notification', 'Channel_referral',
       'Channel_referral scheme', 'Channel_sms', 'Channel_social',
       'units * discount', 'discount vs avg','flash','NC', 'month', 'Quarter', 'Day', 'Week',
       'units', 'percentage_discount', 'AUV', 'revenue', 'RRP',
       'myprotein_rev', 'bfsd_rev', 'vit_rev', 'clothing_rev', 'ha_rev',
       'veg_rev', 'other_rev', 'PRO_rev', 'null_rev', 'GWP_Units', 'GWP_Value'], axis=1)


import CsPy_Uploading as cs
import os

__file__ = r'C:\Users\Ellis.Brew11\PycharmProjects\RetentionModel\CLV Model'

data_upload = cs.UploadJob(dataframe = output
                          , schema=[
                              ("order_number", "INTEGER"),
                             ("locale_key", "INTEGER"),
                             ("order_date", "DATE"),
                             ("CLV_Prediction", "FLOAT"),
                              ("Channel", "STRING"),
                              ("Device", "STRING")
                         ],
                         # columns='',
                         date_column='order_date',
                         # upload_data_type='',
                         bq_project='agile-bonbon-662',
                         bq_dataset='0_Ellis_B',
                         bq_table='NC_CLV_Predictions',
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
                         set_clear_data_cache=True

                         )
data_upload.run()
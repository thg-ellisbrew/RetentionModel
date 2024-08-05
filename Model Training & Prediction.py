import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split
import CsPy_Uploading as cs
import os

query = """

            SELECT * FROM `0_Ellis_B.Retention_Model_Training_Data*`

            WHERE _TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(DATE_ADD(CURRENT_DATE, INTERVAL -91 DAY), INTERVAL -1 YEAR) AS STRING), '-','') AND REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -91 DAY) AS STRING), '-','')


                """

train = pd.DataFrame(cs.DownloadJob(
    query=query,
    input_data_from='BQ',
    output_data_type='DATAFRAME',
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

train['order_date'] = pd.to_datetime(train['order_date'])
train['month'] = train['order_date'].dt.month
train['month'] = train['month'].astype(str)

train = pd.get_dummies(train, columns=['month'], prefix='month')

GB_train = train[train['locale_key'] == 3]
DE_train = train[train['locale_key'] == 2]
JP_train = train[train['locale_key'] == 13]
US_train = train[train['locale_key'] == 12]

GB_NC_train = GB_train[GB_train['order_sequence_no'] == 1]
DE_NC_train = DE_train[DE_train['order_sequence_no'] == 1]
JP_NC_train = JP_train[JP_train['order_sequence_no'] == 1]
US_NC_train = US_train[US_train['order_sequence_no'] == 1]

GB_RC_train = GB_train[GB_train['order_sequence_no'] != 1]
DE_RC_train = DE_train[DE_train['order_sequence_no'] != 1]
JP_RC_train = JP_train[JP_train['order_sequence_no'] != 1]
US_RC_train = US_train[US_train['order_sequence_no'] != 1]

## DROPPING COLUMNS WE DONT WANT TO USE FOR TRAINING THE MODEL

## DROPPED FLASH IND AND SINGLES IND FROM GB,DE,US AS THEY ARE SPECIFIC TO JP


GB_NC_features_train, GB_NC_features_test, GB_NC_target_train, GB_NC_target_test = train_test_split(GB_NC_train.drop(
    ['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind',
     'Golden_Week_Ind'], axis=1), GB_NC_train['Retention90_Ind'], test_size=0.2, random_state=42)
DE_NC_features_train, DE_NC_features_test, DE_NC_target_train, DE_NC_target_test = train_test_split(DE_NC_train.drop(
    ['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind',
     'Golden_Week_Ind'], axis=1), DE_NC_train['Retention90_Ind'], test_size=0.2, random_state=42)
JP_NC_features_train, JP_NC_features_test, JP_NC_target_train, JP_NC_target_test = train_test_split(
    JP_NC_train.drop(['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no'], axis=1),
    JP_NC_train['Retention90_Ind'], test_size=0.2, random_state=42)
US_NC_features_train, US_NC_features_test, US_NC_target_train, US_NC_target_test = train_test_split(US_NC_train.drop(
    ['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind',
     'Golden_Week_Ind'], axis=1), US_NC_train['Retention90_Ind'], test_size=0.2, random_state=42)

GB_RC_features_train, GB_RC_features_test, GB_RC_target_train, GB_RC_target_test = train_test_split(
    GB_RC_train.drop(['Retention90_Ind', 'locale_key', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1),
    GB_RC_train['Retention90_Ind'], test_size=0.2, random_state=42)
DE_RC_features_train, DE_RC_features_test, DE_RC_target_train, DE_RC_target_test = train_test_split(
    DE_RC_train.drop(['Retention90_Ind', 'locale_key', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1),
    DE_RC_train['Retention90_Ind'], test_size=0.2, random_state=42)
JP_RC_features_train, JP_RC_features_test, JP_RC_target_train, JP_RC_target_test = train_test_split(
    JP_RC_train.drop(['Retention90_Ind', 'locale_key', ], axis=1), JP_RC_train['Retention90_Ind'], test_size=0.2,
    random_state=42)
US_RC_features_train, US_RC_features_test, US_RC_target_train, US_RC_target_test = train_test_split(
    US_RC_train.drop(['Retention90_Ind', 'locale_key', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1),
    US_RC_train['Retention90_Ind'], test_size=0.2, random_state=42)

GB_NC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
DE_NC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
JP_NC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
US_NC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')

GB_RC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
DE_RC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
JP_RC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')
US_RC_rf = xgb.XGBClassifier(objective='binary:logistic', use_label_encoder=False, eval_metric='logloss')

## INITIALISING OUR SELECTED MODEL, WE HAVE A MODEL FOR EACH LOCALE AS THEY HAVE UNIQUE BEHAVIOURS

## FITTING MODEL TO OUR TRAINING DATA
GB_RC_rf.fit(GB_RC_features_train.drop('order_date', axis=1), GB_RC_target_train)
DE_RC_rf.fit(DE_RC_features_train.drop('order_date', axis=1), DE_RC_target_train)
US_RC_rf.fit(US_RC_features_train.drop('order_date', axis=1), US_RC_target_train)
JP_RC_rf.fit(JP_RC_features_train.drop('order_date', axis=1), JP_RC_target_train)

GB_NC_rf.fit(GB_NC_features_train.drop('order_date', axis=1), GB_NC_target_train)
DE_NC_rf.fit(DE_NC_features_train.drop('order_date', axis=1), DE_NC_target_train)
US_NC_rf.fit(US_NC_features_train.drop('order_date', axis=1), US_NC_target_train)
JP_NC_rf.fit(JP_NC_features_train.drop('order_date', axis=1), JP_NC_target_train)





#query = """
 #           SELECT * FROM `0_Ellis_B.orders_to_predict*`
#
 #           WHERE _TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -2 DAY) AS STRING), '-','') AND REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY) AS STRING), '-','')
#
 #           """
"""
test = pd.DataFrame(cs.DownloadJob(
    query=query,
    input_data_from='BQ',
    output_data_type='DATAFRAME',
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

"""

test = pd.read_csv(r'orders_to_predict.csv')

test['order_date'] = pd.to_datetime(test['order_date'])

for month in range(1, 13):
    test[f'month_{month}'] = test['order_date'].dt.month == month

test = test[['order_number', 'locale_key', 'order_date', 'volume', 'LY_Retention',
             'Black_Friday_Weekend', 'Impact_Week_Ind', 'Quarter', 'Day', 'Week',
             'Payday_Ind', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind',
             'offer_ind', 'order_sequence_no', 'NC', 'units', 'revenue',
             'percentage_discount', 'AUV', 'myprotein_rev', 'bfsd_rev', 'vit_rev',
             'clothing_rev', 'ha_rev', 'veg_rev', 'other_rev', 'PRO_rev', 'null_rev',
             'customer_lifetime', 'order_frequency', 'month_1',
             'month_10', 'month_11', 'month_12', 'month_2', 'month_3', 'month_4',
             'month_5', 'month_6', 'month_7', 'month_8', 'month_9']]

test_GB = test[test['locale_key'] == 3]
test_DE = test[test['locale_key'] == 2]
test_JP = test[test['locale_key'] == 13]
test_US = test[test['locale_key'] == 12]

test_GB_NC = test_GB[test_GB['order_sequence_no'] == 1]
test_DE_NC = test_DE[test_DE['order_sequence_no'] == 1]
test_JP_NC = test_JP[test_JP['order_sequence_no'] == 1]
test_US_NC = test_US[test_US['order_sequence_no'] == 1]

test_GB_RC = test_GB[test_GB['order_sequence_no'] != 1]
test_DE_RC = test_DE[test_DE['order_sequence_no'] != 1]
test_JP_RC = test_JP[test_JP['order_sequence_no'] != 1]
test_US_RC = test_US[test_US['order_sequence_no'] != 1]


test_GB_NC_new = test_GB[test_GB['order_sequence_no'] == 1]
test_DE_NC_new = test_DE[test_DE['order_sequence_no'] == 1]
test_JP_NC_new = test_JP[test_JP['order_sequence_no'] == 1]
test_US_NC_new = test_US[test_US['order_sequence_no'] == 1]

test_GB_RC_new = test_GB[test_GB['order_sequence_no'] != 1]
test_DE_RC_new = test_DE[test_DE['order_sequence_no'] != 1]
test_JP_RC_new = test_JP[test_JP['order_sequence_no'] != 1]
test_US_RC_new = test_US[test_US['order_sequence_no'] != 1]




test_GB_NC_new['prediction'] = GB_NC_rf.predict_proba(test_GB_NC.drop(
    ['order_number', 'order_date', 'locale_key', 'order_frequency', 'order_sequence_no', 'Flash_Ind', 'Singles_Ind',
     'Golden_Week_Ind'], axis=1))[:, 1]
test_DE_NC_new['prediction'] = DE_NC_rf.predict_proba(test_DE_NC.drop(
    ['order_number', 'order_date', 'locale_key', 'order_frequency', 'order_sequence_no', 'Flash_Ind', 'Singles_Ind',
     'Golden_Week_Ind'], axis=1))[:, 1]
test_JP_NC_new['prediction'] = JP_NC_rf.predict_proba(
    test_JP_NC.drop(['order_number', 'order_date', 'locale_key', 'order_frequency', 'order_sequence_no'], axis=1))[:, 1]
test_US_NC_new['prediction'] = US_NC_rf.predict_proba(test_US_NC.drop(
    ['order_number', 'order_date', 'locale_key', 'order_frequency', 'order_sequence_no', 'Flash_Ind', 'Singles_Ind',
     'Golden_Week_Ind'], axis=1))[:, 1]

test_GB_RC_new['prediction'] = GB_RC_rf.predict_proba(
    test_GB_RC.drop(['order_number', 'order_date', 'locale_key', 'Flash_Ind', 'Singles_Ind', 'Golden_Week_Ind'],
                    axis=1))[:, 1]
test_DE_RC_new['prediction'] = DE_RC_rf.predict_proba(
    test_DE_RC.drop(['order_number', 'order_date', 'locale_key', 'Flash_Ind', 'Singles_Ind', 'Golden_Week_Ind'],
                    axis=1))[:, 1]
test_JP_RC_new['prediction'] = JP_RC_rf.predict_proba(
    test_JP_RC.drop(['order_number', 'order_date', 'locale_key'], axis=1))[:, 1]
test_US_RC_new['prediction'] = US_RC_rf.predict_proba(
    test_US_RC.drop(['order_number', 'order_date', 'locale_key', 'Flash_Ind', 'Singles_Ind', 'Golden_Week_Ind'],
                    axis=1))[:, 1]

dfs = [test_GB_NC_new, test_GB_RC_new, test_DE_NC_new, test_DE_RC_new, test_JP_NC_new, test_JP_RC_new, test_US_NC_new,
       test_US_RC_new]

full_test = pd.concat(dfs, ignore_index=True)

upload_bq = cs.UploadJob(dataframe=full_test,

                         schema=[
                             ("order_number", "INTEGER"),
                             ("locale_key", "INTEGER"),
                             ("order_date", "DATE"),
                             ("volume", "FLOAT"),
                             ("LY_Retention", "FLOAT"),
                             ("Black_Friday_Weekend", "INTEGER"),
                             ("Impact_Week_Ind", "INTEGER"),
                             ("Quarter", "INTEGER"),
                             ("Day", "INTEGER"),
                             ("Week", "INTEGER"),
                             ("Payday_Ind", "INTEGER"),
                             ("Singles_Ind", "INTEGER"),
                             ("Flash_Ind", "INTEGER"),
                             ("Golden_Week_Ind", "INTEGER"),
                             ("offer_ind", "INTEGER"),
                             ("order_sequence_no", "INTEGER"),
                             ("NC", "INTEGER"),
                             ("units", "INTEGER"),
                             ("revenue", "FLOAT"),
                             ("percentage_discount", "FLOAT"),
                             ("AUV", "FLOAT"),
                             ("myprotein_rev", "FLOAT"),
                             ("bfsd_rev", "FLOAT"),
                             ("vit_rev", "FLOAT"),
                             ("clothing_rev", "FLOAT"),
                             ("ha_rev", "FLOAT"),
                             ("veg_rev", "FLOAT"),
                             ("other_rev", "FLOAT"),
                             ("PRO_rev", "FLOAT"),
                             ("null_rev", "FLOAT"),
                             ("customer_lifetime", "INTEGER"),
                             ("order_frequency", "FLOAT"),
                             ("month_1", "INTEGER"),
                             ("month_10", "INTEGER"),
                             ("month_11", "INTEGER"),
                             ("month_12", "INTEGER"),
                             ("month_2", "INTEGER"),
                             ("month_3", "INTEGER"),
                             ("month_4", "INTEGER"),
                             ("month_5", "INTEGER"),
                             ("month_6", "INTEGER"),
                             ("month_7", "INTEGER"),
                             ("month_8", "INTEGER"),
                             ("month_9", "INTEGER"),
                             ("prediction", "FLOAT")

                         ],
                         # columns='',
                         date_column='order_date',
                         # upload_data_type='',
                         bq_project='agile-bonbon-662',
                         bq_dataset='0_Ellis_B',
                         bq_table='Retention_Predictions',
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

upload_bq.run()











import pandas as pd
import xgboost as xgb
from sklearn.model_selection import train_test_split



train = pd.read_csv('Retention_Model_Training_test.csv')

train = train.drop('Unnamed: 0', axis=1)

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


GB_NC_features_train, GB_NC_features_test, GB_NC_target_train, GB_NC_target_test = train_test_split(GB_NC_train.drop(['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), GB_NC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
DE_NC_features_train, DE_NC_features_test, DE_NC_target_train, DE_NC_target_test = train_test_split(DE_NC_train.drop(['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), DE_NC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
JP_NC_features_train, JP_NC_features_test, JP_NC_target_train, JP_NC_target_test = train_test_split(JP_NC_train.drop(['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no'], axis=1), JP_NC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
US_NC_features_train, US_NC_features_test, US_NC_target_train, US_NC_target_test = train_test_split(US_NC_train.drop(['Retention90_Ind', 'locale_key', 'order_frequency', 'order_sequence_no', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), US_NC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)

GB_RC_features_train, GB_RC_features_test, GB_RC_target_train, GB_RC_target_test = train_test_split(GB_RC_train.drop(['Retention90_Ind', 'locale_key', 'Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), GB_RC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
DE_RC_features_train, DE_RC_features_test, DE_RC_target_train, DE_RC_target_test = train_test_split(DE_RC_train.drop(['Retention90_Ind', 'locale_key','Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), DE_RC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
JP_RC_features_train, JP_RC_features_test, JP_RC_target_train, JP_RC_target_test = train_test_split(JP_RC_train.drop(['Retention90_Ind', 'locale_key',], axis=1), JP_RC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)
US_RC_features_train, US_RC_features_test, US_RC_target_train, US_RC_target_test = train_test_split(US_RC_train.drop(['Retention90_Ind', 'locale_key','Singles_Ind', 'Flash_Ind', 'Golden_Week_Ind'], axis=1), US_RC_train['Retention90_Ind'], test_size = 0.2, random_state = 42)



GB_NC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
DE_NC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
JP_NC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
US_NC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')


GB_RC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
DE_RC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
JP_RC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')
US_RC_rf = xgb.XGBClassifier(objective = 'binary:logistic', use_label_encoder=False, eval_metric = 'logloss')



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

import pickle

with open('GB_RC_rf.pkl', 'wb') as file:
    pickle.dump(GB_RC_rf, file)

with open('DE_RC_rf.pkl', 'wb') as file:
    pickle.dump(DE_RC_rf, file)

with open('JP_RC_rf.pkl', 'wb') as file:
    pickle.dump(JP_RC_rf, file)

with open('US_RC_rf.pkl', 'wb') as file:
    pickle.dump(US_RC_rf, file)

with open('GB_NC_rf.pkl', 'wb') as file:
    pickle.dump(GB_NC_rf, file)

with open('DE_NC_rf.pkl', 'wb') as file:
    pickle.dump(DE_NC_rf, file)

with open('JP_NC_rf.pkl', 'wb') as file:
    pickle.dump(JP_NC_rf, file)

with open('US_NC_rf.pkl', 'wb') as file:
    pickle.dump(US_NC_rf, file)














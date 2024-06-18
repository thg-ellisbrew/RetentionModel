import CsPy_Uploading as cs
import os
import pandas as pd


### THIS SCRIPT PULLS THE TRAINING DATA INTO A STATIC TABLE IN BIGQUERY SO THAT THE NEXT SCRIPT CAN PULL THE DATA QUICKLY AND EASILY TO USE FOR MACHINE LEARNING MODEL TRAINING.
### USES BIGQUERY TO PULL DATA AND UPLOADS DATA TO A TABLE IN 0_Ellis_B


query = """
WITH Data AS 

-- PULLING ORDER INFO THAT WE WANT TO USE AS FEATURES
-- FOR V1 OF THE MODEL WE WILL USE THE TOTAL UNITS SPLIT BY CATEGORY TO GET AN IDEA OF HOW BASKETS AFFECT RETENTION
-- THIS IS THE MAIN FOCUS OF MODEL IMPROVEMENT

(


SELECT DISTINCT 


                t.Locale_key
                , T.Order_Number
                , t.Customer_key
                , Order_sequence_no
                , CASE WHEN odh.order_number IS NOT NULL THEN 1 ELSE 0 END AS Offer_Ind
                , CASE WHEN order_sequence_no = 1 THEN 1 ELSE 0 END AS NC
                , SUM(net_qty) AS units
                , SUM(net_qty * unit_charge) revenue
                , SUM(net_qty * unit_RRP) AS RRP
                , SUM(net_qty * unit_mark_down) as markdown
                , SUM(net_qty * unit_discount) as total_discount_value


                , order_date


                , CASE WHEN SUM(unit_discount) > 0 THEN 1 ELSE 0 END AS Discount_Ind



                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Myprotein' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS Myprotein_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Clothing' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS Clothing_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Vitamins' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS Vit_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'BFSD' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS BFSD_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Hard Accessories' THEN net_qty ELSE 0 END) / SUM(net_qty))  AS HA_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Myvegan' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS veg_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'Other' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS Other_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category IS NULL THEN net_qty ELSE 0 END) / SUM(net_qty)) AS Null_rev
                , SUM(net_qty * unit_charge) * (SUM(CASE WHEN Reporting_category = 'PRO' THEN net_qty ELSE 0 END) / SUM(net_qty)) AS PRO_rev

                , SUM(CASE WHEN Reporting_category = 'Myprotein' THEN net_qty ELSE 0 END) / SUM(net_qty) AS Myprotein_cat
                , SUM(CASE WHEN Reporting_category = 'Clothing' THEN net_qty ELSE 0 END) / SUM(net_qty) AS Clothing_cat
                , SUM(CASE WHEN Reporting_category = 'Vitamins' THEN net_qty ELSE 0 END) / SUM(net_qty) AS Vit_cat
                , SUM(CASE WHEN Reporting_category = 'BFSD' THEN net_qty ELSE 0 END) / SUM(net_qty) AS BFSD_cat
                , SUM(CASE WHEN Reporting_category = 'Hard Accessories' THEN net_qty ELSE 0 END) / SUM(net_qty)  AS HA_cat
                , SUM(CASE WHEN Reporting_category = 'Myvegan' THEN net_qty ELSE 0 END) / SUM(net_qty) AS veg_cat
                , SUM(CASE WHEN Reporting_category = 'Other' THEN net_qty ELSE 0 END) / SUM(net_qty) AS Other_cat
                , SUM(CASE WHEN Reporting_category IS NULL THEN net_qty ELSE 0 END) / SUM(net_qty) AS Null_cat
                , SUM(CASE WHEN Reporting_category = 'PRO' THEN net_qty ELSE 0 END) / SUM(net_qty) AS PRO_cat

                , cf.AOV
                , t.order_sequence_no * cf.AOV as CLV
                , DATE_DIFF(t.order_date, cf.first_order_placed, DAY) customer_lifetime
                , DATE_DIFF(t.order_date, cf.first_order_placed, DAY) / t.order_sequence_no as order_frequency




FROM Ditto_HQDW.Transactions t 
LEFT JOIN Ditto_HQDW.Customer_D C
ON c.customer_key = t.customer_key
LEFT JOIN Ditto_HQDW.Product_D p
ON p.product_id = t.ordered_product_id 

LEFT JOIN `Nutrition_Data.Nutrition_Product_Buckets` n
  ON n.product_id = t.ordered_product_id 


LEFT JOIN Ditto_HQDW.Customer_F cf
  ON cf.customer_key = t.customer_key

LEFT JOIN `Offers.Order_Discount_History_*` odh
  ON odh.order_number = t.order_number
  AND odh._TABLE_SUFFIX BETWEEN '20230101' AND '20231231'


WHERE t.Site_key = 46
AND order_date BETWEEN '2023-01-01' AND '2023-12-31' -- ENSURE RECENCY FOR UPDATED VIEW OF BUSINESS PERFORMANCE
AND locale_key IN (2,3,12,13)
AND net_qty > 0
AND order_status_key NOT IN (4,5)
AND order_payment_status_key = 0
AND ordered_free_gift_qty = 0 -- NO FREE GIFTS
AND c.customer_key != -1 -- NO TIKTOK SHOP ORDERS TO AVOID METRIC SKEW
AND order_sequence_no < 200
AND cf.total_orders > 0

GROUP BY 
        locale_key
        , customer_key
        , order_number 
        , order_sequence_no
        , order_date  
        , cf.total_orders
        , cf.AOV
        , t.order_sequence_no * cf.AOV
        , receive_newsletter_key
        , DATE_DIFF(t.order_date, cf.first_order_placed, DAY) / t.order_sequence_no
        , DATE_DIFF(t.order_date, cf.first_order_placed, DAY)
        , CASE WHEN order_sequence_no = 1 THEN 1 ELSE 0 END
        , CASE WHEN odh.order_number IS NOT NULL THEN 1 ELSE 0 END

)

-- FINDING CUSTOMERS WHO HAVE RETURNED, TO GET AN IDEA OF WHEN THEY WILL RETURN 

-- NOT ONLY DO WE WANT TO SEE IF THEY RETURN, BUT ALSO WHEN
-- SO WE CAN CREATE AN AGGREGATED PROBABILITY DISTRIBUTION 

, R AS (
  SELECT DISTINCT 
                   d.order_number
                  , t.order_date as date_of_return
                  , DATE_DIFF(t.order_date, d.order_date, DAY) AS days_to_return
                  , t.order_sequence_no

        FROM Data d 
            
            INNER JOIN Ditto_HQDW.Transactions t 
              ON t.customer_key = d.customer_key 

        WHERE 1=1
          AND t.locale_key IN (2,3,12,13)
          AND DATE_DIFF(t.order_date, d.order_date, DAY) <= 90
          AND t.order_sequence_no = d.order_sequence_no + 1
          AND t.order_date BETWEEN '2023-01-01' AND DATE_ADD('2023-12-31', INTERVAL 90 DAY)
          AND site_key = 46
          AND net_qty > 0
          AND order_status_key NOT IN (4,5)
          AND order_payment_status_key = 0
          AND t.customer_key != -1
)



SELECT DISTINCT  
                locale_key
                , order_date
                , CASE WHEN EXTRACT(MONTH FROM Order_date) = 11 AND EXTRACT(DAY FROM Order_Date) BETWEEN 23 and 29 AND FORMAT_DATE('%A', order_date) = 'Friday' THEN 1 ELSE 0 END AS Black_Friday_Ind
                , CASE WHEN EXTRACT(WEEK FROM order_date) IN (19,20,37,38) THEN 1 ELSE 0 END AS Impact_Week_Ind
                , EXTRACT(MONTH FROM order_date) as month
                , EXTRACT(QUARTER FROM order_date) as Quarter
                , EXTRACT(DAY FROM order_Date) as Day
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Monday' THEN 1 ELSE 0 END AS Is_Monday 
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Tuesday' THEN 1 ELSE 0 END AS Is_Tuesday 
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Wednesday' THEN 1 ELSE 0 END AS Is_Wednesday 
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Thursday' THEN 1 ELSE 0 END AS Is_Thursday
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Friday' THEN 1 ELSE 0 END AS Is_Friday 
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Saturday' THEN 1 ELSE 0 END AS Is_Saturday 
                , CASE WHEN FORMAT_DATE('%A', order_date) = 'Sunday' THEN 1 ELSE 0 END AS Is_Sunday 

                , d.order_sequence_no
                , NC
                , units
                , ROUND(revenue, 2) as revenue
                , discount_ind
                , total_discount_value
                , total_discount_value / (RRP - markdown) as percentage_discount 
                , OFFER_IND
                , revenue / units as AUV
                , myprotein_cat
                , bfsd_cat
                , vit_cat
                , clothing_cat
                , ha_cat
                , veg_cat
                , other_cat
                , PRO_cat
                , null_cat
                , myprotein_rev
                , bfsd_rev
                , vit_rev
                , clothing_rev
                , ha_rev
                , veg_rev
                , other_rev
                , PRO_rev
                , null_rev
                , AOV
                , CLV
                , customer_lifetime
                , order_frequency
                , CASE WHEN r.order_number IS NULL THEN 0 
                       WHEN r.order_number IS NOT NULL THEN 1 
                       END AS Retention90_Ind

    FROM Data d 

        LEFT JOIN R   
          ON r.order_number = d.order_number






"""

data_upload = cs.DownloadJob(
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
    set_clear_save_file_location=False
)
pd.DataFrame(data_upload.run()).to_csv('Retention_Model_Testing.csv')
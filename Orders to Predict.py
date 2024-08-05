import CsPy_Uploading as cs
import os
import pandas as pd

### THIS SCRIPT PULLS THE TRAINING DATA INTO A STATIC TABLE IN BIGQUERY SO THAT THE NEXT SCRIPT CAN PULL THE DATA QUICKLY AND EASILY TO USE FOR MACHINE LEARNING MODEL TRAINING.
### USES BIGQUERY TO PULL DATA AND UPLOADS DATA TO A TABLE IN 0_Ellis_B


query = """
WITH 

Dates as (

  SELECT 
          DATE_ADD(CURRENT_DATE, INTERVAL -4 DAY) AS Start_date
          , DATE_ADD(CURRENT_DATE, INTERVAL -1 DAY) AS End_date

)

, Data AS 

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



                , SUM(CASE WHEN Reporting_category = 'Myprotein' THEN net_qty * unit_charge ELSE 0 END) AS Myprotein_rev
                , SUM(CASE WHEN Reporting_category = 'Clothing' THEN net_qty * unit_charge ELSE 0 END) AS Clothing_rev
                , SUM(CASE WHEN Reporting_category = 'Vitamins' THEN net_qty * unit_charge ELSE 0 END) AS Vit_rev
                , SUM(CASE WHEN Reporting_category = 'BFSD' THEN net_qty * unit_charge ELSE 0 END) AS BFSD_rev
                , SUM(CASE WHEN Reporting_category = 'Hard Accessories' THEN net_qty * unit_charge ELSE 0 END) AS HA_rev
                , SUM(CASE WHEN Reporting_category = 'Myvegan' THEN net_qty * unit_charge ELSE 0 END) AS veg_rev
                , SUM(CASE WHEN Reporting_category = 'Other' THEN net_qty * unit_charge ELSE 0 END) AS Other_rev
                , SUM(CASE WHEN Reporting_category IS NULL THEN net_qty * unit_charge ELSE 0 END) AS Null_rev
                , SUM(CASE WHEN Reporting_category = 'PRO' THEN net_qty * unit_charge ELSE 0 END) AS PRO_rev

                , DATE_DIFF(t.order_date, cf.first_order_placed, DAY) customer_lifetime
                , DATE_DIFF(t.order_date, cf.first_order_placed, DAY) / t.order_sequence_no as order_frequency




FROM Ditto_HQDW.Transactions t , dates as d

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

WHERE t.Site_key = 46
AND order_date BETWEEN d.start_date AND d.end_date -- ENSURE RECENCY FOR UPDATED VIEW OF BUSINESS PERFORMANCE
AND locale_key IN (2,3,12,13)
AND net_qty > 0
AND order_status_key NOT IN (4,5)
AND order_payment_status_key = 0
AND ordered_free_gift_qty = 0 -- NO FREE GIFTS
AND c.customer_key != -1 -- NO TIKTOK SHOP ORDERS TO AVOID METRIC SKEW
AND order_sequence_no < 200
AND cf.total_orders > 0
AND odh._TABLE_SUFFIX BETWEEN REPLACE(CAST(d.start_date as string), '-','') AND REPLACE(CAST(d.end_date AS STRING), '-','')

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


, LY_Retention AS (

  WITH Orders AS (

  SELECT DISTINCT 
                  order_number
                  , locale_key
                  , order_date 
                  , order_sequence_no
                  , customer_key


            FROM Ditto_HQDW.Transactions t , dates as d 

              WHERE 1=1
                AND site_key = 46
                AND locale_key IN (2,3,12,13)
                AND order_date BETWEEN DATE_ADD(d.start_date, INTERVAL -1 YEAR) AND DATE_ADD(d.end_date, INTERVAL -1 YEAR)

  )

  , R AS (

    SELECT DISTINCT 
                    o.order_number

            FROM Ditto_HQDW.Transactions t , dates as d

              INNER JOIN Orders o
                ON o.customer_key = t.customer_key


            WHERE 1=1
              AND site_key = 46
              AND T.locale_key IN (2,3,12,13)
              AND t.order_date BETWEEN DATE_ADD(d.start_date, INTERVAL -1 YEAR) AND DATE_ADD(DATE_ADD(d.end_date, INTERVAL -1 YEAR), INTERVAL 90 DAY)
              AND t.order_sequence_no > o.order_sequence_no
              AND DATE_DIFF(t.order_date, o.order_date, DAY) <= 90

  )

  SELECT DISTINCT 
                  Locale_key
                  , RIGHT(CAST(order_date AS STRING), 5) date
                  , COUNT(DISTINCT r.order_number) / COUNT(DISTINCT o.order_number) as retention

                  FROM orders o

                    LEFT JOIN R 
                      ON r.order_number = o.order_number


                  GROUP BY locale_key, RIGHT(CAST(order_date AS STRING), 5)
  )






, order_volumes AS (

SELECT locale_key, date, orders / SUM(orders) OVER (PARTITION BY locale_key) as volume

FROM (

  SELECT DISTINCT 
                  Locale_key 
                  , RIGHT(CAST(order_date AS STRING), 5) as date
                  , COUNT(DISTINCT order_number) as orders


          FROM Ditto_HQDW.Transactions , Dates as d


          WHERE 1=1
            AND site_key = 46
            AND locale_key IN (2,3,12,13)
            AND order_date BETWEEN DATE_ADD(d.start_date, INTERVAL -2 YEAR) AND DATE_ADD(d.end_date, INTERVAL -1 YEAR)


          GROUP BY RIGHT(CAST(order_date AS STRING), 5), locale_key


)

)

, paydays AS (


SELECT
Max(CAST(Full_Date AS Date)) AS Date

 FROM `Nutrition_Data.Date_D` , dates as d
 WHERE Day_Name_Of_Week IN ('Friday')
 AND Calendar_Year IN (EXTRACT(YEAR FROM Start_date), EXTRACT(YEAR FROM end_date))

GROUP BY
Day_name_of_week,
EXTRACT(MONTH FROM Full_Date),
EXTRACT(YEAR FROM Full_Date)


)


SELECT DISTINCT  
                d.order_number
                , d.locale_key
                , order_date
                , volume
                , LY.Retention as LY_Retention
                , CASE WHEN EXTRACT(MONTH FROM Order_date) = 11 AND EXTRACT(DAY FROM Order_Date) BETWEEN 15 and 29 THEN 1 ELSE 0 END AS Black_Friday_Weekend
                , CASE WHEN EXTRACT(MONTH FROM order_date) IN (5) AND EXTRACT(DAY FROM order_date) BETWEEN 15 AND 30 THEN 1 ELSE 0 END AS Impact_Week_Ind
                , EXTRACT(QUARTER FROM order_date) as Quarter
                , EXTRACT(DAY FROM order_Date) as Day
                , EXTRACT(WEEK FROM order_date) as Week
                , CASE WHEN p.date IS NOT NULL THEN 1 ELSE 0 END AS Payday_Ind
                , CASE WHEN EXTRACT(MONTH FROM order_date) = 11 AND EXTRACT(DAY FROM order_date) = 11 THEN 1 ELSE 0 END AS Singles_Ind
                , CASE WHEN EXTRACT(DAY FROM order_Date) = 11 THEN 1 ELSE 0 END AS Flash_Ind
                , CASE WHEN EXTRACT(MONTH FROM order_Date) = 4 AND EXTRACT(DAY FROM Order_Date) IN (29,30) THEN 1 
                       WHEN EXTRACT(MONTH FROM order_date) = 5 AND EXTRACT(DAY FROM order_date) BETWEEN 1 AND 6 THEN 1
                  ELSE 0 
                  END AS Golden_Week_Ind
                , offer_ind
                , d.order_sequence_no
                , NC
                , units
                , ROUND(revenue, 2) as revenue
                , total_discount_value / (RRP - markdown) as percentage_discount 
                , revenue / units as AUV
                , myprotein_rev
                , bfsd_rev
                , vit_rev
                , clothing_rev
                , ha_rev
                , veg_rev
                , other_rev
                , PRO_rev
                , null_rev
                , customer_lifetime
                , order_frequency


    FROM Data d 

        LEFT JOIN Paydays p
          ON p.date = d.order_date

        LEFT JOIN Order_volumes OV
          ON ov.date = RIGHT(CAST(order_date AS STRING), 5)
          AND ov.locale_key = d.locale_key

        INNER JOIN LY_Retention LY
          ON LY.date = RIGHT(CAST(d.order_date AS STRING), 5)
          AND LY.locale_key = d.locale_key

"""



data_upload = cs.UploadJob(
    query=query,
   input_data_from='BQ',
    schema= [

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
        ("order_frequency", "FLOAT")
    ],
    date_column='order_date',
    # upload_data_type='',
    bq_project='agile-bonbon-662',
    bq_dataset='0_Ellis_B',
    bq_table='orders_to_predict',
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


data_download = pd.DataFrame(cs.DownloadJob(query = query
                               , input_data_from='BQ'
                               , output_data_type='DATAFRAME').run())

data_download.to_csv(r"orders_to_predict.csv")


#data_upload.run()




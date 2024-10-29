import CsPy_Uploading as cs
import os

query = """
WITH IO AS
    (
      SELECT
      DISTINCT
          t.Order_Number
      FROM `Influencer_Data_Copy.Influencer_Transactions` as t
      JOIN `Nutrition_Data.Nutrition_Site_D` as sd
        ON t.Site_Key = sd.Site_Key

      WHERE 1=1
        AND t.boardpack_flag = 1
        AND ((t.attribution = 'Last Click') OR (t.attribution = 'Assisted' AND t.dual_tracking = 'DUAL'))
    )
  ,


  /** Elysium Last Click Channel and Platform **/
  Elysium_Orders AS
    (
      SELECT
      DISTINCT
        odf.Order_Number
        ,LOWER(odf.platform) AS Platform
        ,LOWER(odf.DeviceCategory) AS Device 
        ,LOWER(cade.Channel) AS Channel
        ,LOWER(cade.Source) as Source 
        ,LOWER(odf.campaign) as Campaign
        ,LOWER(cade.Medium) as Medium
      FROM `Central_Marketing.v2_Order_Details_F_THG_Sites_*` as odf
      JOIN `Nutrition_Data.Nutrition_Site_D` as sd
        ON odf.Site_Key = sd.Site_Key
      LEFT JOIN `Central_Marketing.v2_Channel_Attribution_D_Elysium` as cade
        ON odf.Channel_Attribution_Id = cade.Channel_Attribution_Id

      WHERE 1=1
        /** Pulling -1 Day From Automated Date Range To Account for BST as Order Details F is Suffixed on UTC **/
        AND odf._TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY), INTERVAL -1 YEAR) AS STRING), '-', '') AND REPLACE(CAST(CURRENT_DATE AS STRING), '-', '')
    )
  ,


 Customers AS (

  SELECT DISTINCT 

                 p.*
                 , t.Locale
                 , t.customer_key
                 , cd.customer_ID
                 ,CASE WHEN io.Order_Number IS NOT NULL THEN 'influencer'
              WHEN eo.Channel = 'push notification' THEN eo.Channel
              WHEN eo.Medium = 'messaging_app' THEN 'messaging app' /** Fixing an Attribution In Source Table **/
              WHEN eo.platform <> 'web' AND eo.Channel <> 'affiliate' AND t.Order_Date < '2024-03-01' THEN 'mobile app' /** Mobile App Manual Override For Historic Re-Attribution **/
              ELSE LOWER(IFNULL(eo.Channel,'Not Tracked'))
                END AS Channel
                , eo.platform
                , eo.device
                , eo.campaign
                , eo.source
                ,  CASE WHEN ck.Cluster_Name IS NULL THEN 'Newly Motivated' ELSE ck.Cluster_Name END AS Cluster
                , CASE WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed >= DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'New_to_Nutrition'
                WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed < DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'Re-energised'
                ELSE ck.Sub_Cluster_Name END AS Sub_Cluster

          FROM `0_Ellis_B.Retention_Predictions*` P

            INNER JOIN `Ditto_HQDW.Transactions` t
              ON t.order_number = CAST(p.order_number AS STRING)

            LEFT JOIN IO 
              ON IO.order_number = t.order_number 

            LEFT JOIN elysium_orders eo
              ON eo.order_number = t.order_number 

            LEFT JOIN `agile-bonbon-662.Nutrition_Data.Nutrition_Customer_Personas_Model_Global_*` cm
              ON cm.customer_id = t.Elysium_Customer_Id
              AND DATE_TRUNC(t.order_Date, Month) = DATE_TRUNC(cm.Timeframe, Month)

            LEFT JOIN `agile-bonbon-662.Nutrition_Data.Cluster_Keys` ck
              ON ck.Sub_Cluster_Key = cm.Sub_Cluster

            LEFT JOIN `agile-bonbon-662.Ditto_HQDW.Customer_D` cd
              ON t.elysium_customer_id = cd.Customer_Id

            LEFT JOIN `agile-bonbon-662.Ditto_HQDW.Customer_F` f
              ON f.Customer_Key = cd.Customer_Key

            

          WHERE 1=1
            AND t.site_key = 46
            AND t.locale_key IN (2,3,12,13)
            AND t.order_date BETWEEN DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY) AND CURRENT_DATE 
            AND p._TABLE_SUFFIX BETWEEN REPLACE(CAST(DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY) AS STRING), '-','') AND REPLACE(CAST(CURRENT_DATE AS STRING),'-','')

)

, R AS (

  SELECT DISTINCT 
                  c.Order_Number 

          FROM CUSTOMERS C

            INNER JOIN Ditto_HQDW.Transactions t
              ON t.customer_key = c.customer_key

          WHERE 1=1
            AND t.site_key = 46
            AND t.locale_key IN (2,3,12,13)
            AND t.order_date BETWEEN DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY) AND CURRENT_DATE  
            AND DATE_DIFF(t.order_date, c.order_date, day) <= 90
            AND t.order_sequence_no = c.order_sequence_no + 1
)

, R180 AS (

  SELECT DISTINCT 
                  c.Order_Number 

          FROM CUSTOMERS C

            INNER JOIN Ditto_HQDW.Transactions t
              ON t.customer_key = c.customer_key

          WHERE 1=1
            AND t.site_key = 46
            AND t.locale_key IN (2,3,12,13)
            AND t.order_date BETWEEN DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY) AND CURRENT_DATE  
            AND DATE_DIFF(t.order_date, c.order_date, day) <= 180
            AND t.order_sequence_no = c.order_sequence_no + 1
)



, LY AS (


WITH 


Training_dates AS (

              SELECT 
                      DATE_ADD(DATE_ADD(CURRENT_DATE, INTERVAL -180 DAY), INTERVAL -1 YEAR) AS Start_Date
                    , DATE_ADD(CURRENT_DATE, INTERVAL -1 YEAR) AS End_Date
                  

)

, `Data` AS 

-- PULLING ORDER INFO THAT WE WANT TO USE AS FEATURES
-- FOR V1 OF THE MODEL WE WILL USE THE TOTAL UNITS SPLIT BY CATEGORY TO GET AN IDEA OF HOW BASKETS AFFECT RETENTION
-- THIS IS THE MAIN FOCUS OF MODEL IMPROVEMENT

(


SELECT DISTINCT 


                t.Locale
                , T.Order_Number
                , c.customer_ID
                , t.customer_key
                , Order_sequence_no
                , CASE WHEN order_sequence_no = 1 THEN 1 ELSE 0 END AS NC
                , SUM(net_qty) AS units
                , SUM(net_qty * unit_charge) revenue
                , SUM(net_qty * unit_RRP) AS RRP
                , SUM(net_qty * unit_mark_down) as markdown
                , SUM(net_qty * unit_discount) as total_discount_value
                , order_date
                , DATE_DIFF(t.order_date, f.first_order_placed, DAY) customer_lifetime
                , DATE_DIFF(t.order_date, f.first_order_placed, DAY) / t.order_sequence_no as order_frequency
                ,CASE WHEN ck.Cluster_Name IS NULL THEN 'Newly Motivated' ELSE ck.Cluster_Name END AS Cluster
                , CASE WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed >= DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'New_to_Nutrition'
                WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed < DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'Re-energised'
                ELSE ck.Sub_Cluster_Name 
                END AS Sub_Cluster

FROM Ditto_HQDW.Transactions t, training_dates as d


LEFT JOIN Ditto_HQDW.Product_D p
ON p.product_id = t.ordered_product_id 

LEFT JOIN `Nutrition_Data.Nutrition_Product_Buckets` n
  ON n.product_id = t.ordered_product_id 

LEFT JOIN `Offers.Order_Discount_History_*` odh
  ON odh.order_number = t.order_number


LEFT JOIN `agile-bonbon-662.Nutrition_Data.Nutrition_Customer_Personas_Model_Global_*` cm
 ON cm.customer_id = t.Elysium_Customer_Id
 AND DATE_TRUNC(t.order_Date, Month) = DATE_TRUNC(cm.Timeframe, Month)

LEFT JOIN `agile-bonbon-662.Nutrition_Data.Cluster_Keys` ck
  ON ck.Sub_Cluster_Key = cm.Sub_Cluster

LEFT JOIN `agile-bonbon-662.Ditto_HQDW.Customer_D` c
  ON t.elysium_customer_id = c.Customer_Id

LEFT JOIN `agile-bonbon-662.Ditto_HQDW.Customer_F` f
  ON f.Customer_Key = c.Customer_Key

WHERE t.Site_key = 46
AND order_date BETWEEN d.start_date AND d.end_date -- ENSURE RECENCY FOR UPDATED VIEW OF BUSINESS PERFORMANCE
AND locale_key IN (2,3,12,13)
AND net_qty > 0
AND order_status_key NOT IN (4,5)
AND order_payment_status_key = 0
AND ordered_free_gift_qty = 0 -- NO FREE GIFTS
AND c.customer_key != -1 -- NO TIKTOK SHOP ORDERS TO AVOID METRIC SKEW
AND order_sequence_no < 200
AND f.total_orders > 0
AND odh._TABLE_SUFFIX BETWEEN REPLACE(CAST(d.start_Date AS STRING), '-', '') AND REPLACE(CAST(d.end_date AS STRING), '-','')

GROUP BY 
        locale
        , order_number 
        , order_sequence_no
        , order_date  
        , f.total_orders
        , f.AOV
        , t.order_sequence_no * f.AOV
        , receive_newsletter_key
        , DATE_DIFF(t.order_date, f.first_order_placed, DAY) / t.order_sequence_no
        , DATE_DIFF(t.order_date, f.first_order_placed, DAY)
        , CASE WHEN order_sequence_no = 1 THEN 1 ELSE 0 END
        , CASE WHEN odh.order_number IS NOT NULL THEN 1 ELSE 0 END
        , customer_ID
        , customer_key
        ,CASE WHEN ck.Cluster_Name IS NULL THEN 'Newly Motivated' ELSE ck.Cluster_Name END
                , CASE WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed >= DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'New_to_Nutrition'
                WHEN ((ck.Sub_Cluster_Name is NULL OR ck. Sub_Cluster_name = 'Newly Motivated') AND f.first_order_placed < DATE_SUB(t.order_date, INTERVAL 365 Day))
                THEN 'Re-energised'
                ELSE ck.Sub_Cluster_Name 
                END


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

        FROM Data d, training_dates as da
            
            INNER JOIN Ditto_HQDW.Transactions t 
              ON t.customer_key = d.customer_key 

        WHERE 1=1
          AND t.locale_key IN (2,3,12,13)
          AND DATE_DIFF(t.order_date, d.order_date, DAY) <= 90
          AND t.order_sequence_no = d.order_sequence_no + 1
          AND t.order_date BETWEEN da.start_date AND DATE_ADD(da.end_date, INTERVAL 90 DAY)
          AND site_key = 46
          AND net_qty > 0
          AND order_status_key NOT IN (4,5)
          AND order_payment_status_key = 0
          AND t.customer_key != -1
)

, R180 AS (
  SELECT DISTINCT 
                   d.order_number
                  , t.order_date as date_of_return
                  , DATE_DIFF(t.order_date, d.order_date, DAY) AS days_to_return
                  , t.order_sequence_no

        FROM Data d, training_dates as da
            
            INNER JOIN Ditto_HQDW.Transactions t 
              ON t.customer_key = d.customer_key 

        WHERE 1=1
          AND t.locale_key IN (2,3,12,13)
          AND DATE_DIFF(t.order_date, d.order_date, DAY) <= 180
          AND t.order_sequence_no = d.order_sequence_no + 1
          AND t.order_date BETWEEN da.start_date AND DATE_ADD(da.end_date, INTERVAL 180 DAY)
          AND site_key = 46
          AND net_qty > 0
          AND order_status_key NOT IN (4,5)
          AND order_payment_status_key = 0
          AND t.customer_key != -1
)



, paydays AS (


SELECT
Max(CAST(Full_Date AS Date)) AS Date
 
 FROM `Nutrition_Data.Date_D`, training_dates as d
 WHERE Day_Name_Of_Week IN ('Friday')
 AND Calendar_Year IN (EXTRACT(YEAR FROM Start_date), EXTRACT(YEAR FROM End_date))
 
GROUP BY
Day_name_of_week,
EXTRACT(MONTH FROM Full_Date),
EXTRACT(YEAR FROM Full_Date)
 

)


SELECT DISTINCT  
                d.locale
                , d.order_number
                , d.customer_ID
                , d.cluster
                , d.sub_cluster
                , order_date
                , CASE WHEN io.Order_Number IS NOT NULL THEN 'influencer'
              WHEN eo.Channel = 'push notification' THEN eo.Channel
              WHEN eo.Medium = 'messaging_app' THEN 'messaging app' /** Fixing an Attribution In Source Table **/
              WHEN eo.platform <> 'web' AND eo.Channel <> 'affiliate' AND Order_Date < '2024-03-01' THEN 'mobile app' /** Mobile App Manual Override For Historic Re-Attribution **/
              ELSE LOWER(IFNULL(eo.Channel,'Not Tracked'))
                END AS Channel
                , eo.platform
                , eo.device
                , eo.campaign
                , eo.source
                , d.order_sequence_no
                , NC
                , units
                , ROUND(revenue, 2) as revenue
                , total_discount_value / (RRP - markdown) as percentage_discount 
                , revenue / units as AUV
                , customer_lifetime
                , order_frequency
                , CASE WHEN r.order_number IS NULL THEN 0 
                       WHEN r.order_number IS NOT NULL THEN 1 
                       END AS Retention90_Ind
                , CASE WHEN r180.order_number IS NULL THEN 0 
                       WHEN r180.order_number IS NOT NULL THEN 1 
                       END AS Retention180_Ind

    FROM `Data` d , training_dates as da

        LEFT JOIN R   
          ON r.order_number = d.order_number

        LEFT JOIN R180 
          ON r180.order_number = d.order_number

        LEFT JOIN elysium_orders eo
          ON eo.order_number = d.order_number 

        LEFT JOIN Paydays p
          ON p.date = d.order_date

            LEFT JOIN IO 
              ON IO.order_number = d.order_number 

)



SELECT DISTINCT

Locale
, order_date
, CAST(c.order_number AS STRING) AS Order_Number
, customer_ID
, c.cluster
, c.sub_cluster
, C.order_sequence_no
, NC
, UPPER(Channel) Channel
, device
, source
, campaign
, platform
, units
, revenue
, percentage_discount
, AUV
, customer_lifetime
, order_frequency
, CASE WHEN r.order_number IS NULL THEN 0 ELSE 1 END AS Retained
, CASE WHEN r180.order_number IS NULL THEN 0 ELSE 1 END AS Retained180
, prediction

FROM Customers c

  LEFT JOIN R 
    ON r.order_number = c.order_number 

  LEFT JOIN R180 
    ON r180.order_number = c.order_number


UNION ALL

SELECT DISTINCT 

 Locale
, order_date
, order_number
, customer_ID
, cluster
, sub_cluster
, order_sequence_no
, NC
, UPPER(Channel) AS Channel
, device
, source
, campaign
, platform
, units
, revenue
, percentage_discount
, AUV
, customer_lifetime
, order_frequency
, retention90_ind as retained
, retention180_ind as retained180
, 0 AS Prediction 

FROM LY
"""


data_upload = cs.UploadJob(

    query = query
    , schema = [
  ("Locale", "STRING")
, ("order_date", "DATE")
, ("Order_Number", "INTEGER")
, ("customer_ID", "INTEGER")
, ("cluster", "STRING")
, ("sub_cluster", "STRING")
, ("order_sequence_no", "INTEGER")
, ("NC", "INTEGER")
, ("Channel", "STRING")
, ("device", "STRING")
, ("source", "STRING")
, ("campaign", "STRING")
, ("platform", "STRING")
, ("units", "INTEGER")
, ("revenue", "FLOAT")
, ("percentage_discount", "FLOAT")
, ("AUV", "FLOAT")
, ("customer_lifetime", "INTEGER")
, ("order_frequency", "FLOAT")
, ("Retained", "INTEGER")
, ("Retained180", "INTEGER")
, ("prediction", "FLOAT")
    ]
    , input_data_from = 'BQ'
    , date_column='order_date'
    # upload_data_type=''
    , bq_project='agile-bonbon-662'
    , bq_dataset='0_Ellis_B'
    , bq_table='Retention_Performance_Tracker_'
    # bq_key_path=''
    # bq_key_name='',
    # bq_upload_type='',
    # sql_server='',
    # sql_key_path='',
    # sql_key_name='',
    , save_file_path=os.path.join(os.path.dirname(__file__), 'CSV/')
    , account_first_name='Ellis'
    , account_surname='Brew'
    # account_file_path='',
    # set_logging=True,
    , set_clear_data_cache=True
)

data_upload.run()
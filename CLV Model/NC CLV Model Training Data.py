import os
import CsPy_Uploading as cs
import pandas as pd

query = """

WITH 


Training_dates AS (

              SELECT 
                      DATE_ADD(CURRENT_DATE, INTERVAL -181 DAY) AS Start_Date
                    , DATE_ADD(CURRENT_DATE, INTERVAL -181 DAY) AS End_Date
)


, IO AS
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
      FROM `Central_Marketing.v2_Order_Details_F_THG_Sites_*` as odf, training_dates as d
      JOIN `Nutrition_Data.Nutrition_Site_D` as sd
        ON odf.Site_Key = sd.Site_Key
      LEFT JOIN `Central_Marketing.v2_Channel_Attribution_D_Elysium` as cade
        ON odf.Channel_Attribution_Id = cade.Channel_Attribution_Id

      WHERE 1=1
        /** Pulling -1 Day From Automated Date Range To Account for BST as Order Details F is Suffixed on UTC **/
        AND odf._TABLE_SUFFIX BETWEEN REPLACE(CAST(start_date AS STRING), '-', '') AND REPLACE(CAST(end_date AS STRING), '-', '')
    )
, Data AS 

-- PULLING ORDER INFO THAT WE WANT TO USE AS FEATURES

(


SELECT DISTINCT 


                t.Locale_key
                , T.Order_Number
                , t.Customer_key
                , order_sequence_no 
                , CASE WHEN order_sequence_no = 1 THEN 1 ELSE 0 END AS NC
                , CASE WHEN io.Order_Number IS NOT NULL THEN 'influencer'
              WHEN eo.Channel = 'push notification' THEN eo.Channel
              WHEN eo.Medium = 'messaging_app' THEN 'messaging app' /** Fixing an Attribution In Source Table **/
              WHEN eo.platform <> 'web' AND eo.Channel <> 'affiliate' AND t.Order_Date < '2024-03-01' THEN 'mobile app' /** Mobile App Manual Override For Historic Re-Attribution **/
              ELSE LOWER(IFNULL(eo.Channel,'Not Tracked')) 
              END AS Channel
                , eo.device
                , order_date
                , SUM(net_qty) AS units
                , SUM(net_qty * unit_charge) revenue
                , SUM(net_qty * unit_RRP) AS RRP
                , SUM(net_qty * unit_discount) as total_discount_value
                , SUM(CASE WHEN Reporting_category = 'Myprotein' THEN net_qty * unit_charge ELSE 0 END) AS Myprotein_rev
                , SUM(CASE WHEN Reporting_category = 'Clothing' THEN net_qty * unit_charge ELSE 0 END) AS Clothing_rev
                , SUM(CASE WHEN Reporting_category = 'Vitamins' THEN net_qty * unit_charge ELSE 0 END) AS Vit_rev
                , SUM(CASE WHEN Reporting_category = 'BFSD' THEN net_qty * unit_charge ELSE 0 END) AS BFSD_rev
                , SUM(CASE WHEN Reporting_category = 'Hard Accessories' THEN net_qty * unit_charge ELSE 0 END) AS HA_rev
                , SUM(CASE WHEN Reporting_category = 'Myvegan' THEN net_qty * unit_charge ELSE 0 END) AS veg_rev
                , SUM(CASE WHEN Reporting_category = 'Other' THEN net_qty * unit_charge ELSE 0 END) AS Other_rev
                , SUM(CASE WHEN Reporting_category IS NULL THEN net_qty * unit_charge ELSE 0 END) AS Null_rev
                , SUM(CASE WHEN Reporting_category = 'PRO' THEN net_qty * unit_charge ELSE 0 END) AS PRO_rev
                , SUM(ordered_free_gift_qty) as GWP_Units
                , SUM(ordered_free_gift_qty * unit_charge) GWP_value




FROM Ditto_HQDW.Transactions t, training_dates as d

LEFT JOIN Ditto_HQDW.Customer_D C
ON c.customer_key = t.customer_key

LEFT JOIN Ditto_HQDW.Product_D p
ON p.product_id = t.ordered_product_id 

LEFT JOIN `Nutrition_Data.Nutrition_Product_Buckets` n
  ON n.product_id = t.ordered_product_id 

LEFT JOIN Ditto_HQDW.Customer_F cf
  ON cf.customer_key = t.customer_key

LEFT JOIN IO  
  ON io.order_number = t.order_number 

LEFT JOIN elysium_orders eo
  ON eo.order_number = t.order_number 

WHERE t.Site_key = 46
AND order_date BETWEEN d.start_date AND d.end_date -- ENSURE RECENCY FOR UPDATED VIEW OF BUSINESS PERFORMANCE
AND locale_key IN (2,3,12,13)
AND net_qty > 0
AND order_status_key NOT IN (4,5)
AND order_payment_status_key = 0
AND c.customer_key != -1 -- NO TIKTOK SHOP ORDERS TO AVOID METRIC SKEW
AND cf.total_orders > 0
AND t.order_sequence_no = 1

GROUP BY 
        1,2,3,4,5,6,7,8
)

-- FINDING CUSTOMERS WHO HAVE RETURNED


, R AS (

SELECT DISTINCT 

                    d.order_number 
                    , SUM(net_qty * (unit_charge)) as subsequent_revenue
                    
                    
                    FROM Data d, training_dates as da
                    
                        INNER JOIN Ditto_HQDW.Transactions t
                            ON t.customer_key = d.customer_key
                            AND t.site_key =46
                            AND t.order_date BETWEEN da.start_date AND DATE_ADD(da.end_date, INTERVAL 180 DAY)
                            AND DATE_DIFF(t.order_date, d.order_date, DAY) <= 180
                            AND d.order_Sequence_no > t.order_sequence_no 
                            AND t.net_qty > 0
                            AND t.order_Status_key NOT IN (4,5)
                            AND t.order_payment_status_key = 0
                            AND t.locale_key IN (2,3,12,13)
                            
                            
                    GROUP BY 1
                    
                    )        
                            
                    

SELECT DISTINCT  
                 d.locale_key
                , order_date
                , NC
                , Channel
                , device
                , EXTRACT(MONTH FROM order_date) as month
                , EXTRACT(QUARTER FROM order_date) as Quarter
                , EXTRACT(DAY FROM order_Date) as Day
                , EXTRACT(WEEK FROM order_date) as Week
                , units
                , total_discount_value / (RRP) as percentage_discount 
                , revenue / units as AUV
                , revenue as revenue 
                , RRP AS RRP
                , myprotein_rev
                , bfsd_rev
                , vit_rev
                , clothing_rev
                , ha_rev
                , veg_rev
                , other_rev
                , PRO_rev
                , null_rev
                , GWP_Units
                , GWP_Value
                , Revenue + COALESCE(subsequent_revenue, 0) as CLV_180
                , CASE WHEN r.order_number IS NOT NULL THEN 1 
                        ELSE 0
                        END AS Retention_180



    FROM Data d 
    
        LEFT JOIN R 
            ON r.order_number = d.order_number 



"""

CLV_download = cs.UploadJob(query=query,
                            schema=[
                                ("locale_key", "INTEGER"),
                                ("order_date", "DATE"),
                                ("NC", "INTEGER"),
                                ("Channel", "STRING"),
                                ("device", "STRING"),
                                ("month", "INTEGER"),
                                ("Quarter", "INTEGER"),
                                ("Day", "INTEGER"),
                                ("Week", "INTEGER"),
                                ("units", "INTEGER"),
                                ("percentage_discount", "FLOAT"),
                                ("AUV", "FLOAT"),
                                ("revenue", "FLOAT"),
                                ("RRP", "FLOAT"),
                                ("myprotein_rev", "FLOAT"),
                                ("bfsd_rev", "FLOAT"),
                                ("vit_rev", "FLOAT"),
                                ("clothing_rev", "FLOAT"),
                                ("ha_rev", "FLOAT"),
                                ("veg_rev", "FLOAT"),
                                ("other_rev", "FLOAT"),
                                ("PRO_rev", "FLOAT"),
                                ("null_rev", "FLOAT"),
                                ("GWP_Units", "INTEGER"),
                                ("GWP_Value", "FLOAT"),
                                ("CLV_180", "FLOAT"),
                                ("Retention_180", "INTEGER")

                            ],
                            # columns='',
                            date_column='order_date',
                            # upload_data_type='',
                            bq_project='agile-bonbon-662',
                            bq_dataset='0_Ellis_B',
                            bq_table='NC_CLV_Model_Training_Data_',
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
                            set_clear_data_cache=True)

CLV_download.run()




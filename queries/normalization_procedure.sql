--CALL `task3_dataset.sp_normalize_apple_data`();

CREATE OR REPLACE PROCEDURE `task3_dataset.sp_normalize_apple_data`()
BEGIN

  -- 1. Dim Apps
  MERGE `task3_dataset.dim_apps` T
  USING (
    SELECT 
      App_Apple_ID as app_apple_id, 
      MAX(App_Name) as app_name
    FROM `task3_dataset.apple_sales_reports`
    WHERE App_Apple_ID IS NOT NULL
    GROUP BY App_Apple_ID
  ) S
  ON T.app_apple_id = S.app_apple_id
  WHEN MATCHED THEN
    UPDATE SET app_name = S.app_name
  WHEN NOT MATCHED THEN
    INSERT (app_apple_id, app_name)
    VALUES (S.app_apple_id, S.app_name);

  -- 2. Dim Subscriptions
  MERGE `task3_dataset.dim_subscriptions` T
  USING (
    SELECT 
      Subscription_Apple_ID as subscription_apple_id,
      MAX(Subscription_Name) as subscription_name,
      MAX(Subscription_Group_ID) as subscription_group_id,
      MAX(Subscription_Duration) as subscription_duration
    FROM `task3_dataset.apple_sales_reports`
    WHERE Subscription_Apple_ID IS NOT NULL
    GROUP BY Subscription_Apple_ID
  ) S
  ON T.subscription_apple_id = S.subscription_apple_id
  WHEN MATCHED THEN
    UPDATE SET 
      subscription_name = S.subscription_name,
      subscription_duration = S.subscription_duration
  WHEN NOT MATCHED THEN
    INSERT (subscription_apple_id, subscription_name, subscription_group_id, subscription_duration)
    VALUES (S.subscription_apple_id, S.subscription_name, S.subscription_group_id, S.subscription_duration);

  -- 3. Dim Subscribers
  MERGE `task3_dataset.dim_subscribers` T
  USING (
    SELECT 
      Subscriber_ID as subscriber_id,
      MAX(Subscriber_ID_Reset) as subscriber_id_reset -- Якщо є 'Yes' і NULL, MAX вибере 'Yes'
    FROM `task3_dataset.apple_sales_reports`
    WHERE Subscriber_ID IS NOT NULL
    GROUP BY Subscriber_ID
  ) S
  ON T.subscriber_id = S.subscriber_id
  WHEN MATCHED THEN
    UPDATE SET subscriber_id_reset = S.subscriber_id_reset
  WHEN NOT MATCHED THEN
    INSERT (subscriber_id, subscriber_id_reset)
    VALUES (S.subscriber_id, S.subscriber_id_reset);

  -- 4. Facts
  MERGE `task3_dataset.fact_subscription_events` T
  USING (
    SELECT DISTINCT
      CAST(Event_Date AS DATE) as event_date,
      App_Apple_ID as app_apple_id,
      Subscription_Apple_ID as subscription_apple_id,
      Subscriber_ID as subscriber_id,
      Customer_Price as customer_price,
      Developer_Proceeds as developer_proceeds,
      Units as units,
      Customer_Currency as customer_currency,
      Proceeds_Currency as proceeds_currency,
      Device as device,
      Country as country,
      Client as client,
      Introductory_Price_Type as introductory_price_type,
      Introductory_Price_Duration as introductory_price_duration,
      CAST(Marketing_Opt_In_Duration AS STRING) as marketing_opt_in_duration,
      Preserved_Pricing as preserved_pricing,
      Proceeds_Reason as proceeds_reason,
      Refund as refund,
      SAFE_CAST(Purchase_Date AS DATE) as purchase_date
    FROM `task3_dataset.apple_sales_reports`
    WHERE Event_Date IS NOT NULL AND Subscriber_ID IS NOT NULL
  ) S
  ON T.event_date = S.event_date 
     AND T.subscriber_id = S.subscriber_id 
     AND T.subscription_apple_id = S.subscription_apple_id
     AND COALESCE(T.introductory_price_type, 'Regular') = COALESCE(S.introductory_price_type, 'Regular')

  WHEN MATCHED THEN
    UPDATE SET 
      customer_price = S.customer_price,
      developer_proceeds = S.developer_proceeds,
      refund = S.refund
      
  WHEN NOT MATCHED THEN
    INSERT (
      event_date, app_apple_id, subscription_apple_id, subscriber_id,
      customer_price, developer_proceeds, units,
      customer_currency, proceeds_currency, device, country, client,
      introductory_price_type, introductory_price_duration, marketing_opt_in_duration,
      preserved_pricing, proceeds_reason, refund, purchase_date
    )
    VALUES (
      S.event_date, S.app_apple_id, S.subscription_apple_id, S.subscriber_id,
      S.customer_price, S.developer_proceeds, S.units,
      S.customer_currency, S.proceeds_currency, S.device, S.country, S.client,
      S.introductory_price_type, S.introductory_price_duration, S.marketing_opt_in_duration,
      S.preserved_pricing, S.proceeds_reason, S.refund, S.purchase_date
    );

END;
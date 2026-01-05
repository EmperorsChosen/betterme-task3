

-- 1. Довідник додатків
CREATE OR REPLACE TABLE `task3_dataset.dim_apps` (
    app_apple_id INT64 OPTIONS(description="Unique Apple ID of the app"),
    app_name STRING OPTIONS(description="Name of the app")
);

-- 2. Довідник підписок
CREATE OR REPLACE TABLE `task3_dataset.dim_subscriptions` (
    subscription_apple_id INT64 OPTIONS(description="Unique Apple ID of the subscription product"),
    subscription_name STRING,
    subscription_group_id INT64,
    subscription_duration STRING OPTIONS(description="Duration: 7 Days, 1 Month, etc.")
);

-- 3. Довідник користувачів
CREATE OR REPLACE TABLE `task3_dataset.dim_subscribers` (
    subscriber_id INT64 OPTIONS(description="Unique anonymized user ID"),
    subscriber_id_reset STRING OPTIONS(description="Flag if ID was reset (Yes/Blank)")
);

-- 4. Таблиця подій
CREATE OR REPLACE TABLE `task3_dataset.fact_subscription_events` (
    event_date DATE,
    app_apple_id INT64,         
    subscription_apple_id INT64,
    subscriber_id INT64,        
    
    customer_price FLOAT64,
    developer_proceeds FLOAT64,
    units FLOAT64,              
    
    customer_currency STRING,
    proceeds_currency STRING,
    
    device STRING,
    country STRING,
    client STRING,
  
    introductory_price_type STRING, 
    introductory_price_duration STRING,
    marketing_opt_in_duration STRING,
    
    preserved_pricing STRING,
    proceeds_reason STRING,
    refund STRING,                      
    purchase_date DATE                   
);
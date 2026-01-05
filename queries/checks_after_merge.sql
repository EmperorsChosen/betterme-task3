-- перевірки після мерджу з роу дати в нашу структуру вже
-- записи мають збігатись
SELECT 
  'Raw Data' as Source,
  COUNT(*) as Total_Rows,
  ROUND(SUM(Developer_Proceeds), 2) as Total_Revenue
FROM `task3_dataset.apple_sales_reports`
UNION ALL
SELECT 
  'Normalized Fact Table' as Source,
  COUNT(*) as Total_Rows,
  ROUND(SUM(developer_proceeds), 2) as Total_Revenue
FROM `task3_dataset.fact_subscription_events`;


-- пари мають збігатись
SELECT 
  (SELECT COUNT(*) FROM `task3_dataset.dim_apps`) as Apps_Count,
  (SELECT COUNT(DISTINCT App_Apple_ID) FROM `task3_dataset.apple_sales_reports`) as Raw_Apps_Check,
  
  (SELECT COUNT(*) FROM `task3_dataset.dim_subscriptions`) as Subs_Count,
  (SELECT COUNT(DISTINCT Subscription_Apple_ID) FROM `task3_dataset.apple_sales_reports`) as Raw_Subs_Check,

  (SELECT COUNT(*) FROM `task3_dataset.dim_subscribers`) as Users_Count,
  (SELECT COUNT(DISTINCT Subscriber_ID) FROM `task3_dataset.apple_sales_reports`) as Raw_Users_Check;

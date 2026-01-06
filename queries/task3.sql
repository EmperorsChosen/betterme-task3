--3.3. Знайти тривалість підписки з заданим <id>
/* припущення: під <id> мається на увазі айді підписки, тобто
    унікальний <id> підписки як класу, а не конкретного інстансу.
   Додав перерахування у дні для зручності через регулярні вирази.*/
SELECT 
  subscription_apple_id,
  subscription_duration,
  
  CAST(REGEXP_EXTRACT(subscription_duration, r'(\d+)') AS INT64) *
  CASE 
    WHEN subscription_duration LIKE '%Year%' THEN 365
    WHEN subscription_duration LIKE '%Mont%' THEN 30
    WHEN subscription_duration LIKE '%Week%' THEN 7 
    WHEN subscription_duration LIKE '%Day%' THEN 1
    ELSE 0 
  END AS apx_sub_days_duration

FROM `task3_dataset.dim_subscriptions`
WHERE subscription_apple_id = 1451591605; --Сюди вставити айді підписки, наприклад: 1451591605--


--3.4. Знайти список підписок для додатку з заданим <id>
/* припущення: під <id> мається на увазі айді додатку, а під списком підписок мається
    на увазі список унікальних підписок-класів які були оформлені на цей додаток-продукт*/

SELECT DISTINCT 
  s.subscription_apple_id,
  s.subscription_name 
FROM `task3_dataset.fact_subscription_events` f
JOIN `task3_dataset.dim_subscriptions` s 
  ON f.subscription_apple_id = s.subscription_apple_id
WHERE f.app_apple_id = 1363010081; --Сюди вставити айді додатку, наприклад: 1363010081 BetterMe: Calm,Sleep,Meditate--


--3.5. Визначити який додаток приніс більше доходу за заданий період
/*Я виконую облік коштів за датою фактичної транзакції. Якщо ж метою є оцінка якості трафіку, варто використати віконні функції для виключення користувачів, які здійснили рефанд або чарджбек у майбутньому.
Для обрахування суми була створена таблиця task3_dataset.dim_currency 
шляхом зовнішнього апі. код скрипту у додатках*/
select 
  a.app_name,
  
  round(sum(
    f.developer_proceeds 
    * coalesce(c.rate_to_usd, 1)
    * case 
        when f.refund = 'Yes' then -1
        else 1 
      end
  ), 2) as total_revenue_usd

from `task3_dataset.fact_subscription_events` f
join `task3_dataset.dim_apps` a
  on f.app_apple_id = a.app_apple_id
left join `task3_dataset.dim_currency` c
  on f.proceeds_currency = c.currency_code

where f.event_date between '2019-02-01' and '2019-02-10'
group by a.app_name 
order by total_revenue_usd desc;


--3.6. Знайти конверсію з оформлення пробного періоду в успішний платіж для користувачів, що зробили підписку <id> в дату <date>
/*Спочатку селектимо тих хто взяв підписку - на відміну від того шаблону 
що був прикладений до тестового, тут структура звіту стара. І інформація про те траял це чи ні міститься не там де зараз. Тому ми спочатку аналізуємо по айдішці субскрайбера чи є по ньому ще записи за допомогою джоїна таблиці саму на себе. Можна було б через віконну функцію, але мені здалося так попростіше структурно. І далі вже якщо є записи - розраховуємо % коверсії. Важливо: якщо користувач не відмінив траял, а потім оформив рефанд - все одно його рахуємо. бо це вже не відповідає на питання про конверсію. */
with trial_cohort as (
  -- 1. Знаходимо тих, хто стартував тріал
  select distinct 
    subscriber_id,
    event_date as start_date
  from `task3_dataset.fact_subscription_events`
  where subscription_apple_id = 1447369566 
    and event_date = '2019-02-02'
    and introductory_price_type = 'Free Trial'
),

successful_conversions as (
  -- 2. Шукаємо серед них тих, хто хоч раз заплатив
  select distinct 
    t.subscriber_id
  from trial_cohort t
  join `task3_dataset.fact_subscription_events` s
    on t.subscriber_id = s.subscriber_id
  where s.event_date > t.start_date
    and s.customer_price > 0
)

-- 3. Рахуємо цифри
select 
  (select count(*) from trial_cohort) as trials_started,
  (select count(*) from successful_conversions) as paid_conversions,
  round(
    safe_divide(
      (select count(*) from successful_conversions),
      (select count(*) from trial_cohort)
    ) * 100, 2
  ) as conversion_rate_percent




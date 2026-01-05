Перед виконанням скриптів і запитів та процедур необхідно створити проект та датасет у BigQuery.
Потім створити сервісний акаунт з адмін.правами на базу. створити ключ, прописати до нього шлях
Прописати це в відповідних місцях у обох скриптах

Для виконання завдання номер 3 було сторено скрипт для завантаження у БД (для прикладу розглядалась BigQuery - даний скрипт не вносить ніяких модифікацій, він просто записує всі дані з архіву в початкову таблицю)

Потім необхідно запустити скрипт courses.py - він затягне в базу і створить таблицю з курсами валют до долара. Це потрібно для 

Всі подальші трансофрмації я вирішив робити безспосередньо в БД.
Скрипти SQL виконуються в такому порядку безпосередньо в базі:
1. database_tables_creation.sql - створення таблиць для бд
2. normalization_procedure.sql - процедура мерджу
3. checks_after_merge.sql - перевірка на цілісність даних після мерджу
4. task3.sql - запити до задачі 3. П. 3.3 - 3.6


Архітектура даних: зірка. дані зі звіту були поділені на Fact Table та Dimension Tables
Причина: оптимально для OLAP (думаю в даному випадку у нас саме аналітична платформа)

Схема:

```mermaid
erDiagram
    dim_apps ||--|{ fact_subscription_events : "has"
    dim_subscriptions ||--|{ fact_subscription_events : "sold in"
    dim_subscribers ||--|{ fact_subscription_events : "generates"
    dim_currency ||--|{ fact_subscription_events : "converts"

    dim_apps {
        INT64 app_apple_id PK
        STRING app_name
    }

    dim_subscriptions {
        INT64 subscription_apple_id PK
        STRING subscription_name
        STRING subscription_duration
    }

    dim_subscribers {
        INT64 subscriber_id PK
        STRING subscriber_id_reset
    }

    dim_currency {
        STRING currency_code PK
        FLOAT64 rate_to_usd
    }

    fact_subscription_events {
        DATE event_date
        INT64 app_apple_id FK
        INT64 subscription_apple_id FK
        INT64 subscriber_id FK
        STRING proceeds_currency FK
        FLOAT64 developer_proceeds
        STRING introductory_price_type
        STRING refund
    }

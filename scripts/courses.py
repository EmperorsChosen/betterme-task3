import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
from datetime import datetime

KEY_PATH = 'D:/Work/BetterMe 2025/betterme-3-task-9c2948b1a562.json'
PROJECT_ID = 'betterme-3-task'       
DATASET_ID = 'task3_dataset'             
TABLE_ID = 'dim_currency'              

API_URL = "https://open.er-api.com/v6/latest/USD"

def update_exchange_rates():
    print("Отримуємо курси валют...")
    try:
        response = requests.get(API_URL)
        data = response.json()
        
        if data['result'] != 'success':
            print("Помилка API:", data)
            return
            
        rates = data['rates'] 
        
    except Exception as e:
        print(f"Не вдалося підключитися до API: {e}")
        return

    df = pd.DataFrame(list(rates.items()), columns=['currency_code', 'rate_per_usd'])
    
    df['updated_at'] = datetime.now()
    
    df['rate_to_usd'] = 1 / df['rate_per_usd']
    
    print(f"Отримано {len(df)} валют.")

    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE", 
        autodetect=True
    )

    try:
        job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f" Таблиця {TABLE_ID} оновлена.")
    except Exception as e:
        print(f"Помилка BigQuery: {e}")

if __name__ == '__main__':
    update_exchange_rates()
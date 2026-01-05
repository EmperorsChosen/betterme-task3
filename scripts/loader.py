import zipfile
import pandas as pd
import io
import os
from google.cloud import bigquery
from google.oauth2 import service_account


ZIP_PATH = 'D:/Work/BetterMe 2025/itunes_dataset.zip'
KEY_PATH = 'D:/Work/BetterMe 2025/betterme-3-task-9c2948b1a562.json'
PROJECT_ID = 'betterme-3-task'
DATASET_ID = 'task3_dataset'
TABLE_ID = 'apple_sales_reports'

def load_to_bigquery():
    credentials = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)

    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND", 
        autodetect=True,
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION]
    )

    if not os.path.exists(ZIP_PATH):
        print(f"Помилка: Архів {ZIP_PATH} не знайдено.")
        return
    
    with zipfile.ZipFile(ZIP_PATH, 'r') as z:
        file_list = z.namelist()
        
        for filename in file_list:
            if filename.endswith('.txt'):
                print(f"{filename}")
                
                with z.open(filename) as f:
                    try:
                        df = pd.read_csv(f, sep='\t')
                    except Exception as e:
                        print(f"Не вдалося прочитати {filename}: {e}")
                        continue

                    df.columns = [c.strip().replace(' ', '_').replace('-', '_') for c in df.columns]

                    if df.empty:
                        print("Файл порожній")
                        continue


                    if 'Event_Date' in df.columns:
                        df['Event_Date'] = pd.to_datetime(df['Event_Date'], errors='coerce')

                    print(f"{len(df)} рядків")


                    try:
                        job = client.load_table_from_dataframe(
                            df, table_ref, job_config=job_config
                        )
                        job.result() 
                        print(f" Файл {filename} завантажено")
                    except Exception as e:
                        print(e)

if __name__ == '__main__':
    load_to_bigquery()
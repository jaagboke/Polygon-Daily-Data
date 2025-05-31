from datetime import timedelta
from datetime import date, datetime
from polygon.rest import  RESTClient
import uuid
from azure.storage.filedatalake import (
    DataLakeServiceClient
)
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO #Import io
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from airflow import DAG
from airflow.operators.python import PythonOperator
from dags.config import configurations

def get_market_data(client):
    data = client.get_grouped_daily_aggs(
        str((date.today() - timedelta(days=1)).strftime("%Y-%m-%d")),
        adjusted=True
    )
    return data

def format_market_data(data: dict):
    formatted_data = []
    for record in data:
        formatted_record = {
            'id': str(uuid.uuid4()),
            'ticker': record.ticker,
            'open': record.open,
            'high': record.high,
            'low': record.low,
            'close': record.close,
            'volume': record.volume,
            'vwap': record.vwap,
            'timestamp': record.timestamp,
            'transactions': record.transactions,
            'otc': record.otc,

        }
        formatted_data.append(formatted_record)
    return formatted_data

def get_blob_service_client(account_name):
    account_url = f"https://{account_name}.dfs.core.windows.net"
    credential = DefaultAzureCredential()

    container_client = BlobServiceClient(account_url, credential=credential)
    return container_client

def get_data_to_gen2():
    api_key = configurations['api_key']
    client = RESTClient(api_key)
    try:
        market_data = get_market_data(client)
        formatted_market_data = format_market_data(market_data)

        #Convert the market data into a dataframe
        df = pd.DataFrame(formatted_market_data)
        #Modify the timestamp column to a datetime
        df['timestamp'] = pd.to_datetime(df['timestamp'], unit='ms', errors='coerce')
        #Convert the pandas df to a pyarrow
        table = pa.Table.from_pandas(df)

        #Initialise the bytes
        df_buffer = BytesIO()
        #Write the bytes array to a parquet
        pq.write_table(table, df_buffer)
        df_buffer.seek(0)

        # Set the Path
        account_name = configurations['account_name']
        account_key = configurations['account_key']

        # Authenticate to ADLS Gen 2
        # Authenticate the storage account and key
        service_client = DataLakeServiceClient(account_url=f"https://{account_name}.dfs.core.windows.net",
                                               credential=account_key)

        # Get the storage container (Bronze)
        filesystem_client = service_client.get_file_system_client("bronze")

        # Define todays date
        df_date = str((date.today() - timedelta(days=1)).strftime("%Y-%m-%d"))

        # Specify the path
        path = f"{df_date}.parquet"

        # Get the created file path in the storage account
        file_client = filesystem_client.get_file_client(path)

        # Upload data to container
        file_client.upload_data(df_buffer.read(), overwrite=True)

    except Exception as e:
        print(f"Error!: {e}")

default_args = {
    'owner': 'jaxhacker',
    'start_date': datetime(2025, 5,29),
    'depends_on_past': False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "Get_data_from_API",
    default_args=default_args,
    description="Getting data from Polygon API",
    schedule_interval=None,
    catchup=False

) as dag:
    task = PythonOperator(
        task_id="push_data_to_Gen2",
        python_callable=get_data_to_gen2
    )
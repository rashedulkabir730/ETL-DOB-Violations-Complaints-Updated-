import pandas as pd
import boto3
from sodapy import Socrata
import requests
from io import StringIO
import calendar
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
from sodapy import Socrata

# Initialize boto3 client
s3_client = boto3.client('s3')
target_bucket = 'transf_dob'

# URLs for complaints and violations datasets
complaints_url = 'eabe-havv'
violations_url = 'cepu-5g8r'

def extract_data(limit_set=1000000, chunks=50000):
    client = Socrata("data.ofnewyork.us", None)

    # ---- Fetching complaints data ----
    offset = 0
    complaints_data_chunks = []

    while offset < limit_set:
        results = client.get(complaints_url, limit=chunks, offset=offset)
        if not results:
            break
        complaints_data_chunks.append(pd.DataFrame(results))
        offset += chunks

    complaints_df = pd.concat(complaints_data_chunks, ignore_index=True) if complaints_data_chunks else pd.DataFrame()

    # ---- Fetching violations data ----
    offset = 0
    violations_data_chunks = []

    while offset < limit_set:
        url_1 = f"https://data.ofnewyork.us/resource/{violations_url}.csv?$limit={chunks}&$offset={offset}"
        response_1 = requests.get(url_1)
        if response_1.status_code == 200:
            current_data_1 = pd.read_csv(StringIO(response_1.text))
            if current_data_1.empty:
                break
            violations_data_chunks.append(current_data_1)
            offset += chunks
        else:
            print(f"Failed to retrieve data from violations API. Status code: {response_1.status_code}")
            break

    violations_df = pd.concat(violations_data_chunks, ignore_index=True) if violations_data_chunks else pd.DataFrame()

    # Save files locally
    file_name_com = 'complaints_data.csv'
    complaints_df.to_csv(f"/home/ubuntu/{file_name_com}", index=False)

    file_name_vio = 'violations_data.csv'
    violations_df.to_csv(f"/home/ubuntu/{file_name_vio}", index=False)

    # Return file paths for next task
    return [file_name_com, f"/home/ubuntu/{file_name_com}"], [file_name_vio, f"/home/ubuntu/{file_name_vio}"]

def transform_data(task_instance):
    # Pull the file paths from the extract_data task
    output_list_com, output_list_vio = task_instance.xcom_pull(task_ids='tsk_extract_data')

    # Read the complaints and violations CSVs
    file_name_com, output_file_path_com = output_list_com
    file_name_vio, output_file_path_vio = output_list_vio

    com_df = pd.read_csv(output_file_path_com)
    vio_df = pd.read_csv(output_file_path_vio)

    # Transform complaints
    com_df['full_add'] = com_df['house_number'] + " " + com_df['house_street']
    com_df.drop(columns=['bin', 'community_board', 'special_district', 'disposition_date', 'disposition_code',
                         'dobrundate', 'inspection_date'], axis=1, inplace=True)
    com_df['Complaint Issue Date'] = pd.to_datetime(com_df['date_entered'])
    com_df['Complaint Year'] = com_df['Complaint Issue Date'].dt.year
    com_df['Month'] = com_df['Complaint Issue Date'].dt.month
    com_df['Day'] = com_df['Complaint Issue Date'].dt.day
    cols_to_drop = ['bin', 'community_board','special_district','disposition_date','disposition_code','dobrundate','inspection_date']
    com_df.drop(columns=cols_to_drop, axis=1, inplace=True)

    # Transform violations
    vio_df['full_add'] = vio_df['house_number'] + " " + vio_df['street']
    vio_df['Violation Issue Date'] = pd.to_datetime(vio_df['issue_date'])
    vio_df['Year'] = vio_df['Violation Issue Date'].dt.year
    vio_df['Month'] = vio_df['Violation Issue Date'].dt.month
    vio_df['Day'] = vio_df['Violation Issue Date'].dt.day
    vio_df['description'] = vio_df['description'].fillna("Violation Remarks not provided")

    cols_to_drop = ['boro', 'bin', 'lot', 'block', 'disposition_date', 'disposition_comments', 'device_number',
                    'ecb_number', 'house_number', 'street', 'issue_date', 'isn_dob_bis_viol']
    vio_df.drop(columns=cols_to_drop, axis=1, inplace=True)

    # Save transformed data
    transformed_com_file = f"{file_name_com}_transformed.csv"
    com_df.to_csv(f"/home/ubuntu/{transformed_com_file}", index=False)

    transformed_vio_file = f"{file_name_vio}_transformed.csv"
    vio_df.to_csv(f"/home/ubuntu/{transformed_vio_file}", index=False)

    # Upload transformed files to S3
    s3_client.put_object(Bucket=target_bucket, Key=transformed_com_file, Body=open(f"/home/ubuntu/{transformed_com_file}", 'rb'))
    s3_client.put_object(Bucket=target_bucket, Key=transformed_vio_file, Body=open(f"/home/ubuntu/{transformed_vio_file}", 'rb'))

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(seconds=15)
}

# Define the DAG
with DAG('cam_rev_dag', default_args=default_args, catchup=False) as dag:
    # Extract data from the source
    extract_data_task = PythonOperator(
        task_id='tsk_extract_data',
        python_callable=extract_data
    )

    # Transform the extracted data
    transform_data_task = PythonOperator(
        task_id='tsk_transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    # Load transformed files to S3
    load_to_s3_complaints = BashOperator(
        task_id='tsk_load_complaints_to_s3',
        bash_command='aws s3 mv /home/ubuntu/{{ ti.xcom_pull(task_ids="tsk_transform_data")[0][1] }} s3://transf_dob/'
    )

    load_to_s3_violations = BashOperator(
        task_id='tsk_load_violations_to_s3',
        bash_command='aws s3 mv /home/ubuntu/{{ ti.xcom_pull(task_ids="tsk_transform_data")[1][1] }} s3://transf_dob/'
    )

    # Define task dependencies
    extract_data_task >> transform_data_task >> [load_to_s3_complaints, load_to_s3_violations]
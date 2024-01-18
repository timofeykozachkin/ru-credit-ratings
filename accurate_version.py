import datetime as dt
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# sys.path.append("/opt/hadoop/airflow/dags/tdkozachkin/")
sys.path.append("./")
import commands.agg_data
import commands.data
import commands.send_data
import commands.upload_data

with DAG(
    "credit_ratings_ru_dag",
    schedule_interval="@daily",
    start_date=dt.datetime(2024, 1, 14),
) as dag:
    (
        PythonOperator(
            task_id="get_data", python_callable=commands.data.main
        )
        >> PythonOperator(
            task_id="agg_data_agencies", python_callable=commands.agg_data.main
        )
        >> PythonOperator(
            task_id="to_mysql_ratings", python_callable=commands.upload_data.main
        )
        >> PythonOperator(
            task_id="to_tgchat_message", python_callable=commands.send_data.main
        )
    )

import datetime as dt
import sys

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

sys.path.append("/opt/hadoop/airflow/dags/tdkozachkin/")
import parsers.agg_data
import parsers.data
import parsers.send_data
import parsers.upload_data

with DAG(
    "credit_ratings_ru_dag",
    schedule_interval="@daily",
    start_date=dt.datetime(2024, 1, 14),
) as dag:
    (
        PythonOperator(task_id="get_data", python_callable=parsers.data.main)
        >> PythonOperator(
            task_id="agg_data_agencies", python_callable=parsers.agg_data.main
        )
        >> PythonOperator(
            task_id="to_mysql_ratings", python_callable=parsers.upload_data.main
        )
        >> PythonOperator(
            task_id="to_tgchat_message", python_callable=parsers.send_data.main
        )
    )

# with DAG(
#     dag_id='credit_ratings_ru_dag',
#     start_date=pendulum.datetime(2024, 1, 15, tz='UTC'),
#     schedule_interval='@daily',
#     catchup=False
# ) as dag:
#     @task(task_id='get_data')
#     def get_data(**kwargs):
#         current_date = kwargs['ds']
#         current_date = '2023-12-27'
#         df1 = expert_ra_ratings()
#         # df2 = df1.copy().iloc[0:0]
#         df2 = expert_ra_archive()
#         df3 = nkr_ratings()
#         df4 = nra_ratings()
#         pd_df = pd.concat([df1, df3], ignore_index=True)
#         pd_df = pd.concat([pd_df, df4], ignore_index=True)
#         pd_df = pd.concat([pd_df, df2], ignore_index=True)

#         pd_df['rat_date'] = pd_df['rat_date'].str.replace('.', '-', regex=False)
#         pd_df['rat_date'] = pd.to_datetime(pd_df['rat_date'], format="%d-%m-%Y")

#         pd_current_date = pd.to_datetime(current_date, format='%Y-%m-%d')

#         # pd_df = pd_df[pd_df['rat_date'] == pd_current_date]
#         pd_df = pd_df[pd_df['rat_date'] <= pd_current_date]

#         spark = SparkSession.builder\
#             .master("yarn")\
#             .appName('ratings_task')\
#             .getOrCreate()

#         df = spark.createDataFrame(pd_df)

#         df\
#             .repartition(1)\
#             .write\
#             .mode('overwrite')\
#             .parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

#         spark.stop()

#     @task(task_id="agg_data_agencies")
#     def agg_data(**kwargs):
#         current_date = kwargs['ds']
#         current_date = '2023-12-27'
#         spark = SparkSession.builder\
#                     .master("yarn")\
#                     .appName('ratings_task')\
#                     .getOrCreate()

#         df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

#         df_agg = df\
#                     .groupBy("agency")\
#                     .agg( count(col("name")).alias("num_ratings") )

#         df_agg\
#             .write\
#             .mode("overwrite")\
#             .parquet(f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")

#         spark.stop()

#     @task(task_id="to_mysql_ratings")
#     def upload_to_mysql(**kwargs):
#         current_date = kwargs['ds']
#         current_date = '2023-12-27'

#         spark = SparkSession.builder\
#             .master("yarn")\
#             .appName('ratings_task')\
#             .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
#             .getOrCreate()

#         df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

#         df\
#             .write\
#             .mode("overwrite")\
#             .format("jdbc")\
#             .option("driver", "com.mysql.cj.jdbc.Driver")\
#             .option("url", "jdbc:mysql://localhost:3306/hse")\
#             .option("dbtable", "credit_ratings")\
#             .option("user", "arhimag")\
#             .option("password", "password57")\
#             .save()

#         df_agg = spark.read.parquet(f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")

#         df_agg\
#             .withColumn("record_date", lit(current_date).cast(DateType()))\
#             .write\
#             .mode("overwrite")\
#             .format("jdbc")\
#             .option("driver", "com.mysql.cj.jdbc.Driver")\
#             .option("url", "jdbc:mysql://localhost:3306/hse")\
#             .option("dbtable", "agencies_stats")\
#             .option("user", "arhimag")\
#             .option("password", "password57")\
#             .save()

#         spark.stop()

#     @task(task_id="to_tgchat_message")
#     def tg_bot_message(**kwargs):
#         current_date = kwargs['ds']
#         current_date = '2023-12-27'

#         spark = SparkSession.builder\
#             .master("yarn")\
#             .appName("ratings")\
#             .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
#             .getOrCreate()

#         # agg_df = spark.read.parquet(f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")\
#         #     .toPandas()
#         df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")
#         df = df\
#             .filter(col("rat_date") == lit(current_date))\
#             .toPandas()
#         df['rat_date'] = df['rat_date'].dt.strftime('%Y-%m-%d')

#         if len(df) == 0:
#             message_data = f'No updates on {current_date}'
#             send_tg_message(message_data)
#         else:
#             message = f"Credit ratings Updates on {current_date}:"
#             send_tg_message(message)

#             for i in np.arange(len(df)):
#                 message_data = get_one_item_message(df.iloc[i])
#                 send_tg_message(message_data)

#             # agg_message = "<i>Number of agencies ratings TODAY:</i>\n"
#             # for i in np.arange(len(agg_df)):
#             #     ag = agg_df.iloc[i]
#             #     agg_message += f"{ag['agency']} - {ag['num_ratings']} набл.\n"
#             # send_tg_message(agg_message)

#         spark.stop()

#     get_data() >> agg_data() >> upload_to_mysql() >> tg_bot_message()

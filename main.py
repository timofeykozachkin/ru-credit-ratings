import numpy as np
import pandas as pd
import pendulum
from airflow import DAG
from airflow.decorators import task
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

from parsers.expert_ra import expert_ra_ratings
from parsers.nkr import nkr_ratings
from parsers.nra import nra_ratings
from tg_bot.tg_channel_updates import get_one_item_message, send_tg_message

with DAG(
    dag_id="credit_ratings_ru_dag",
    start_date=pendulum.datetime(2024, 1, 15, tz="UTC"),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    @task(task_id="get_data")
    def get_data(**kwargs):
        current_date = kwargs["ds"]
        current_date = "2023-12-27"
        df1 = expert_ra_ratings()
        # df2 = df1.copy().iloc[0:0]
        # df2 = expert_ra_archive()
        df3 = nkr_ratings()
        df4 = nra_ratings()
        pd_df = pd.concat([df1, df3], ignore_index=True)
        pd_df = pd.concat([pd_df, df4], ignore_index=True)
        # pd_df = pd.concat([pd_df, df2], ignore_index=True)

        pd_df["rat_date"] = pd_df["rat_date"].str.replace(".", "-", regex=False)
        pd_df["rat_date"] = pd.to_datetime(pd_df["rat_date"], format="%d-%m-%Y")

        pd_current_date = pd.to_datetime(current_date, format="%Y-%m-%d")

        pd_df = pd_df[pd_df["rat_date"] >= pd_current_date]
        # pd_df = pd_df[pd_df['rat_date'] <= pd_current_date]

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("ratings_task")
            .getOrCreate()
        )

        df = spark.createDataFrame(pd_df)

        df.repartition(1).write.mode("overwrite").parquet(
            f"/user/tdkozachkin/project/DATA/DT={current_date}"
        )

    @task(task_id="to_mysql_ratings")
    def upload_to_mysql(**kwargs):
        current_date = kwargs["ds"]
        current_date = "2023-12-27"

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("ratings_task")
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")
            .getOrCreate()
        )

        df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

        df.write.mode("append").format("jdbc").option(
            "driver", "com.mysql.cj.jdbc.Driver"
        ).option("url", "jdbc:mysql://localhost:3306/hse").option(
            "dbtable", "credit_ratings"
        ).option(
            "user", "arhimag"
        ).option(
            "password", "password57"
        ).save()

    @task(task_id="to_tgchat_message")
    def tg_bot_message(**kwargs):
        current_date = kwargs["ds"]
        current_date = "2023-12-27"

        spark = (
            SparkSession.builder.master("local[*]")
            .appName("ratings")
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")
            .getOrCreate()
        )

        df = (
            spark.read.format("jdbc")
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("url", "jdbc:mysql://localhost:3306/hse")
            .option("dbtable", "credit_ratings")
            .option("user", "arhimag")
            .option("password", "password57")
            .load()
        )

        df = df.filter(col("rat_date") == lit(current_date)).toPandas()
        df["rat_date"] = df["rat_date"].dt.strftime("%Y-%m-%d")

        if len(df) == 0:
            message_data = f"No updates on {current_date}"
            send_tg_message(message_data)
        else:
            message = f"Credit ratings Updates on {current_date}:"
            send_tg_message(message)

            for i in np.arange(len(df)):
                message_data = get_one_item_message(df.iloc[i])
                send_tg_message(message_data)

    get_data() >> upload_to_mysql() >> tg_bot_message()

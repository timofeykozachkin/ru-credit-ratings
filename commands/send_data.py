import numpy as np
import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit


def send_tg_message(message):
    BOT_TOKEN = "6719020665:AAG9wfbiG-eAa8TP3ZNPdTFw2qI2vt4FcL8"
    CHANNEL_ID = "-1002095900488"
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    data = {"chat_id": CHANNEL_ID, "text": message, "parse_mode": "HTML"}
    response = requests.post(url, data=data)


def get_one_item_message(row):
    message = f"""
    <b>{row['name']}</b>
    <i>rating</i>:             <b>{row['rating']}</b>
    <i>prediction</i>:     {row['pred']}
    <i>rating_date</i>:   {row['rat_date']}
    <i>observation</i>:  {row['observation']}
    <i>agency</i>:           {row['agency']}
    """
    return message


def main(**kwargs):
    current_date = kwargs['ds']

    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("ratings")\
        .config(
            "spark.jars",
            "/usr/share/java/mysql-connector-java-8.2.0.jar")\
        .getOrCreate()

    df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")
    df = df\
        .filter(col("rat_date") == lit(current_date))\
        .toPandas()
    df['rat_date'] = df['rat_date'].dt.strftime('%Y-%m-%d')

    if len(df) == 0:
        message_data = f'No updates on {current_date}'
        send_tg_message(message_data)
    else:
        message = f"Credit ratings Updates on {current_date}:"
        send_tg_message(message)

        for i in np.arange(len(df)):
            message_data = get_one_item_message(df.iloc[i])
            send_tg_message(message_data)

        agg_df = spark.read.parquet(
            f"/user/tdkozachkin/project/AGGDATA/DT={current_date}")\
            .toPandas()
        agg_message = "<i>Number of agencies ratings TODAY:</i>\n"
        for i in np.arange(len(agg_df)):
            ag = agg_df.iloc[i]
            agg_message += f"{ag['agency']} - {ag['num_ratings']} набл.\n"
        send_tg_message(agg_message)

    spark.stop()

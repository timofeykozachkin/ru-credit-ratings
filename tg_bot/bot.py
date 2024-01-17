import telebot
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import numpy as np


def many_items_message(df):
    if len(df) == 0:
        message = "Нет рейтинга"
    else:
        message = "Доступные рейтинги:"
        for i in np.arange(len(df)):
            row = df.iloc[i]
            message += f"""
<b>{row['name']}</b>
<i>rating</i>:             <b>{row['rating']}</b>
<i>prediction</i>:     {row['pred']}
<i>rating_date</i>:   {row['rat_date']}
<i>observation</i>:  {row['observation']}
<i>agency</i>:           {row['agency']}
            """
    return message

token = "6719020665:AAG9wfbiG-eAa8TP3ZNPdTFw2qI2vt4FcL8"
bot = telebot.TeleBot(token, parse_mode="HTML")

@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, "Привет, по твоему названию банка я пришлю доступные для него рейтинги")

@bot.message_handler(content_types=['text'])
def echo(message):
    bot.send_message(message.chat.id, text=f"Ищу...")
    bank_name = (f"{message.text}").lower()

    spark = SparkSession.builder\
            .master("local[*]")\
            .appName("ratings")\
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
            .getOrCreate()

    df = spark\
        .read\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('url', 'jdbc:mysql://localhost:3306/hse')\
        .option('dbtable', 'credit_ratings')\
        .option('user', 'arhimag')\
        .option('password', 'password57')\
        .load()

    df = df\
        .where(col('name').like(f"%{bank_name}%"))\
        .toPandas()
    
    answer = many_items_message(df)
    bot.send_message(message.chat.id, text=answer)

bot.infinity_polling()
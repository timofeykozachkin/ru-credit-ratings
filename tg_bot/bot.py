import telebot
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, lower
import numpy as np
import pandas as pd


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
keyboard = telebot.types.ReplyKeyboardMarkup(row_width=1, resize_keyboard=True)
button1 = telebot.types.KeyboardButton('Найти по дате')
keyboard.add(button1)
button2 = telebot.types.KeyboardButton('Найти по названию')
keyboard.add(button2)


@bot.message_handler(commands=['start'])
def start_message(message):
    bot.send_message(message.chat.id, 
                     "Привет, по твоему названию банка или дате я пришлю доступные для него рейтинги")
    bot.send_message(message.chat.id, 'Выберите действие:', reply_markup=keyboard)


@bot.message_handler(func=lambda message: message.text == 'Найти по дате')
def message_reply_button1(message):
    bot.send_message(message.chat.id, text='Введите дату (YYYY-MM-DD)')

@bot.message_handler(func=lambda message: message.text and '-' in message.text)
def message_reply_date(message):
    bot.send_message(message.chat.id, text=f"Ищу...")
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName('ratings_task')\
        .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
        .getOrCreate()

    df = spark\
        .read\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('url', 'jdbc:mysql://localhost:3306/hse')\
        .option('dbtable', 'kzch_credit_ratings')\
        .option('user', 'arhimag')\
        .option('password', 'password57')\
        .load()

    df = df\
        .where(col('rat_date') == message.text)\
        .toPandas()        
    answer = many_items_message(df)
    bot.send_message(message.chat.id, text=answer, reply_markup=keyboard)
    spark.stop()

@bot.message_handler(func=lambda message: message.text == 'Найти по названию')
def message_reply_button2(message):
    bot.send_message(message.chat.id, text='Введите название')

@bot.message_handler(func=lambda message: message.text)
def message_reply_name(message):   
    bot.send_message(message.chat.id, text=f"Ищу...")
    bank_name = (f"{message.text}").lower()

    spark = SparkSession.builder\
            .master("local[*]")\
            .appName('ratings_task')\
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
            .getOrCreate()

    df = spark\
        .read\
        .format('jdbc')\
        .option('driver', 'com.mysql.cj.jdbc.Driver')\
        .option('url', 'jdbc:mysql://localhost:3306/hse')\
        .option('dbtable', 'kzch_credit_ratings')\
        .option('user', 'arhimag')\
        .option('password', 'password57')\
        .load()

    df = df\
        .where(lower(col('name')).like(f"%{bank_name}%"))\
        .toPandas()
    
    answer = many_items_message(df)
    bot.send_message(message.chat.id, text=answer, reply_markup=keyboard)

    spark.stop()
        
bot.infinity_polling()
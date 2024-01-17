import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
from airflow import DAG
from airflow.decorators import task
import pendulum
import pyspark.sql.functions as F




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


def expert_ra_ratings():
    url = "https://raexpert.ru/ratings/bankcredit_all/"
    response = requests.get(url)

    soup = BeautifulSoup(response.text, 'html.parser')
    tables = soup.find_all('table')
    spans = tables[1].find_all('span')
    ratings = spans[8:]
    result = {'name': [], 'rating': [], 'pred': [],
              'rat_date': [], 'observation': []}

    for i, span in enumerate(ratings):
        if i % 2 == 1:
            observ = '-'
            name = ratings[i-1].find('a').text
            params = span.text.strip().split(',')
            if len(params) == 2:
                pred = '-'
            elif len(params) == 3:
                pred = params[-2].strip()
            elif len(params) == 4:
                pred = params[-2].strip()
                observ = params[1].strip()
            else:
                print(params)
                raise ValueError

            rat = params[0].strip()
            rat_date = params[-1].strip()

            result['name'].append(name)
            result['rating'].append(rat)
            result['pred'].append(pred)
            result['rat_date'].append(rat_date)
            result['observation'].append(observ)

    df = pd.DataFrame(result)
    df['agency'] = 'Эксперт РА'

    return df


def expert_ra_archive():
    result = {'name': [], 'rating': [], 'pred': [],
              'rat_date': [], 'observation': []}

    for i in np.arange(2, 16):
        with open(f'/opt/hadoop/airflow/dags/tdkozachkin/archive/{i}.txt') as f:
            lines = f.read()

        soup = BeautifulSoup(lines, 'html.parser')
        tables = soup.find_all('table')

        spans = tables[1].find_all('span')
        ratings = spans[8:]

        for i, span in enumerate(ratings):
            if i % 2 == 1:
                observ = '-'
                name = ratings[i-1].find('a').text
                params = span.text.strip().split(',')
                if len(params) == 2:
                    pred = '-'
                elif len(params) == 3:
                    pred = params[-2].strip()
                elif len(params) == 4:
                    pred = params[-2].strip()
                    observ = params[1].strip()
                else:
                    print(params)
                    raise ValueError

                rat = params[0].strip()
                rat_date = params[-1].strip()

                result['name'].append(name)
                result['rating'].append(rat)
                result['pred'].append(pred)
                result['rat_date'].append(rat_date)
                result['observation'].append(observ)

    df = pd.DataFrame(result)
    df['agency'] = 'Эксперт РА'

    return df


def nkr_ratings():
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.get("https://ratings.ru/ratings/issuers/")
    time.sleep(3)

    sector = driver.find_element(By.XPATH, "/html/body/div[1]/main/section[1]/div/div[2]/div[3]")
    driver.implicitly_wait(1)
    sector.click()
    bank = driver.find_element(By.XPATH, "/html/body/div[1]/main/section[1]/div/div[2]/div[3]/div[2]/div[1]/label[2]")
    driver.implicitly_wait(1)
    bank.click()
    primenit = driver.find_element(By.XPATH, "/html/body/div[1]/main/section[1]/div/div[2]/div[3]/div[2]/div[2]/button[2]")
    driver.implicitly_wait(1)
    primenit.click()
    exc = driver.find_element(By.XPATH, '//*[@id="issuers-table"]/thead/tr/th[1]')
    driver.implicitly_wait(1)
    exc.click()

    page = driver.page_source
    soup1 = BeautifulSoup(page, "html.parser")
    tables = soup1.find_all('table', {'id': 'issuers-table'})
    dat = tables[0].find_all('td')
    result = {'name': [], 'rating': [], 'pred': [],
              'rat_date': [], 'observation': []}

    try:
        for i, item in enumerate(dat):
            a_div = item.find('a')
            if a_div is not None:
                if str(a_div['href']).startswith("/ratings/issuers/"):
                    result['name'].append(a_div.text)
                    result['observation'].append('-')
                if str(a_div['href']).startswith("/ratings/press-releases/"):
                    result['rat_date'].append(a_div.text)
            else:
                if i % 6 == 1:
                    result['rating'].append(item.find('span').text)
                if i % 6 == 2:
                    result['pred'].append(item.find('span').text)

        driver.quit()
    except:
        print('error')
        driver.quit()

    df = pd.DataFrame(result)
    df['agency'] = 'НКР'

    return df


def nra_ratings():
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.get("https://www.ra-national.ru/ratings/")
    time.sleep(1)
    result = {'name': [], 'rating': [], 'pred': [],
              'rat_date': [], 'observation': []}

    sector = driver.find_element(By.XPATH, '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[8]')
    driver.execute_script("arguments[0].scrollIntoView();", sector)
    time.sleep(3)
    driver.implicitly_wait(1)
    sector.click()

    bank = driver.find_element(By.XPATH, '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[8]/div/div/div/div[2]/div/div/div[1]/label/div')
    driver.execute_script("arguments[0].scrollIntoView();", bank)
    time.sleep(3)
    driver.implicitly_wait(1)
    bank.click()

    primenit = driver.find_element(By.XPATH, '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[11]/div')
    driver.execute_script("arguments[0].scrollIntoView();", primenit)
    time.sleep(3)
    driver.implicitly_wait(1)
    driver.execute_script("arguments[0].click();", primenit)

    more_tab = driver.find_element(By.XPATH, '//*[@id="loadmore"]/div/div')
    for i in np.arange(3):
        driver.execute_script("arguments[0].scrollIntoView();", more_tab)
        time.sleep(3)
        driver.implicitly_wait(1)
        driver.execute_script("arguments[0].click();", more_tab)

    page = driver.page_source
    soup1 = BeautifulSoup(page, "html.parser")

    tables = soup1.find_all('div', {'class': 'jet-listing-grid__items grid-col-desk-1 grid-col-tablet-1 grid-col-mobile-1 jet-listing-grid--712'})
    divs = tables[0].find_all('h2')
    dates = tables[0].find_all('div', {'class': 'jet-listing-dynamic-field__content'})

    for dat in dates:
        if dat.text != '':
            result['rat_date'].append(dat.text)

    for i, item in enumerate(divs):
        if i % 5 == 1:
            result['name'].append(item.text)
        elif i % 5 == 2:
            result['rating'].append(item.text)
        elif i % 5 == 3:
            result['observation'].append(item.text)
        elif i % 5 == 4:
            result['pred'].append(item.text)

    driver.quit()

    df = pd.DataFrame(result)
    df['agency'] = 'НРА'
    return df


with DAG(
    dag_id='credit_ratings_ru_dag',
    start_date=pendulum.datetime(2024, 1, 15, tz='UTC'),
    schedule_interval='@daily',
    catchup=False
) as dag:
    @task(task_id='get_data')
    def get_data(**kwargs):
        current_date = kwargs['ds']
        current_date = '2023-12-27'
        df1 = expert_ra_ratings()
        # df2 = df1.copy().iloc[0:0]
        # df2 = expert_ra_archive()
        df3 = nkr_ratings()
        df4 = nra_ratings()
        pd_df = pd.concat([df1, df3], ignore_index=True)
        pd_df = pd.concat([pd_df, df4], ignore_index=True)
        # pd_df = pd.concat([pd_df, df2], ignore_index=True)

        pd_df['rat_date'] = pd_df['rat_date'].str.replace('.', '-', regex=False)
        pd_df['rat_date'] = pd.to_datetime(pd_df['rat_date'], format="%d-%m-%Y")

        pd_current_date = pd.to_datetime(current_date, format='%Y-%m-%d') - pd.DateOffset(1)

        pd_df = pd_df[pd_df['rat_date'] >= pd_current_date].drop_duplicates()
        # pd_df = pd_df[pd_df['rat_date'] <= pd_current_date]

        spark = SparkSession.builder\
            .master("local[*]")\
            .appName('ratings_task')\
            .getOrCreate()

        df = spark.createDataFrame(pd_df)

        df\
            .repartition(1)\
            .write\
            .mode('overwrite')\
            .parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

    @task(task_id="to_mysql_ratings")
    def upload_to_mysql(**kwargs):
        current_date = kwargs['ds']
        current_date = '2023-12-27'

        spark = SparkSession.builder\
            .master("local[*]")\
            .appName('ratings_task')\
            .config("spark.jars", "/usr/share/java/mysql-connector-java-8.2.0.jar")\
            .getOrCreate()

        df = spark.read.parquet(f"/user/tdkozachkin/project/DATA/DT={current_date}")

        df\
            .write\
            .mode("append")\
            .format("jdbc")\
            .option("driver", "com.mysql.cj.jdbc.Driver")\
            .option("url", "jdbc:mysql://localhost:3306/hse")\
            .option("dbtable", "credit_ratings")\
            .option("user", "arhimag")\
            .option("password", "password57")\
            .save()

    @task(task_id="to_tgchat_message")
    def tg_bot_message(**kwargs):
        current_date = kwargs['ds']
        current_date = '2023-12-27'

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
            .withColumn('rat_date_next', F.date_add(col('rat_date'), 1).cast('timestamp'))\
            .filter(col("rat_date_next") == lit(current_date))\
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

    get_data() >> upload_to_mysql() >> tg_bot_message()

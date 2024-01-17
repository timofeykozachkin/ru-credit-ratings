import requests
from bs4 import BeautifulSoup
import numpy as np
import pandas as pd


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
        with open(
            f'/opt/hadoop/airflow/dags/tdkozachkin/archive/{i}.txt'
        ) as f:
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

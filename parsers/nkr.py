from bs4 import BeautifulSoup
import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver import FirefoxOptions
import time


def nkr_ratings():
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.get("https://ratings.ru/ratings/issuers/")
    time.sleep(3)

    sector = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/main/section[1]/div/div[2]/div[3]")
    driver.implicitly_wait(1)
    sector.click()
    bank = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/main/section[1]/div/div[2]/div[3]/div[2]/div[1]/label[2]")
    driver.implicitly_wait(1)
    bank.click()
    primenit = driver.find_element(
        By.XPATH,
        "/html/body/div[1]/main/section[1]/div/div[2]/div[3]/div[2]/div[2]/button[2]")
    driver.implicitly_wait(1)
    primenit.click()
    exc = driver.find_element(
        By.XPATH,
        '//*[@id="issuers-table"]/thead/tr/th[1]')
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
    except Exception:
        print('error')
        driver.quit()

    df = pd.DataFrame(result)
    df['agency'] = 'НКР'

    return df

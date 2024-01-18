import time

import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver import FirefoxOptions
from selenium.webdriver.common.by import By


def nra_ratings():
    opts = FirefoxOptions()
    opts.add_argument("--headless")
    driver = webdriver.Firefox(options=opts)
    driver.get("https://www.ra-national.ru/ratings/")
    time.sleep(1)
    result = {"name": [], "rating": [], "pred": [], "rat_date": [], "observation": []}

    sector = driver.find_element(
        By.XPATH, '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[8]'
    )
    driver.execute_script("arguments[0].scrollIntoView();", sector)
    time.sleep(3)
    driver.implicitly_wait(1)
    sector.click()

    bank = driver.find_element(
        By.XPATH,
        '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[8]/div/div/div/div[2]/div/div/div[1]/label/div',
    )
    driver.execute_script("arguments[0].scrollIntoView();", bank)
    time.sleep(3)
    driver.implicitly_wait(1)
    bank.click()

    primenit = driver.find_element(
        By.XPATH, '//*[@id="allratings"]/div/div[1]/div/section/div/div/div/div[11]/div'
    )
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

    tables = soup1.find_all(
        "div",
        {
            "class": "jet-listing-grid__items grid-col-desk-1 grid-col-tablet-1 grid-col-mobile-1 jet-listing-grid--712"
        },
    )
    divs = tables[0].find_all("h2")
    dates = tables[0].find_all("div", {"class": "jet-listing-dynamic-field__content"})

    for dat in dates:
        if dat.text != "":
            result["rat_date"].append(dat.text)

    for i, item in enumerate(divs):
        if i % 5 == 1:
            result["name"].append(item.text)
        elif i % 5 == 2:
            result["rating"].append(item.text)
        elif i % 5 == 3:
            result["observation"].append(item.text)
        elif i % 5 == 4:
            result["pred"].append(item.text)

    driver.quit()

    df = pd.DataFrame(result)
    df["agency"] = "НРА"
    return df

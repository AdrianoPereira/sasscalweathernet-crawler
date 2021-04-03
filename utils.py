import os
from string import Template
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


def get_nested_urls(self):
    root_url = "http://sasscalweathernet.org/index.php"
    print(f"Requesting {root_url}...", end=' ')
    res = requests.get(root_url)

    if res.status_code == 200:
        print(f"[{res.status_code}] request successfully...")

        soup = BeautifulSoup(res.text, "html.parser")
        rows = soup.select(
            '#ina > div:nth-child(1) > table'
        )[0].find_all('tr')

        nested_urls = dict()
        for i, row in enumerate(rows):
            if i % 2 == 0:
                country = row.find('img').attrs['title']
                province = row.find('span').get_text().strip()
                if not nested_urls.get(country):
                    nested_urls[country] = dict()
            else:
                links = row.select('#menu_left_st > a')
                links = list(
                    link.attrs['href'] for link in links
                )
                sub_urls = dict(
                    info_url=f"{root_url}/{links[0]}",
                    hourly_url=f"{root_url}/{links[1]}",
                    daily_url=f"{root_url}/{links[2]}",
                    monthly_url=f"{root_url}/{links[3]}",
                    data_url=f"{root_url}/{links[4]}"
                )
                nested_urls[country][province] = sub_urls
        return nested_urls
    else:
        print(f"[{res.status_code}] request failed!")
        return None
import os
from string import Template
import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np


class Crawler:
    def __init__(self, station_id, year, month):
        self.station_id = station_id
        self.year = year
        self.month = str(month).zfill(2)
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.args = dict(
            path=self.path,
            sid=self.station_id,
            year=self.year,
            month=self.month
        )
        self.dirname = Template(
            "$path/$sid/$year/$month/"
        ).substitute(**self.args)
        self.args['dirname'] = self.dirname
        self.filename = Template(
            "$dirname/SID$sid-Y$year-M$month.txt"
        ).substitute(**self.args)

        self.base_url = "http://www.sasscalweathernet.org/weatherstat_daily_we.php"
        self.args['burl'] = self.base_url
        self.params_url = Template(
            "$burl?yrmth_crit=$year-$month&graph_category=1&gesendet=Start+search&loggerid_crit=$sid"
        ).substitute(**self.args)

        self.__make_directories()
        self.__fetch_and_save_data()

    def __make_directories(self):
        if not os.path.exists(self.dirname):
            os.makedirs(self.dirname)

    def __request_data(self):
        print(f"Requesting {self.params_url}...", end=' ')
        res = requests.post(self.params_url)
        if res.status_code == 200:
            print(f"[{res.status_code}] request successfully...")
            table = BeautifulSoup(
                res.text, "html.parser"
            ).select('#inb_1 > table')
            if len(table) == 0:
                return None
            else:
                return table[0]
        else:
            print(f"[{res.status_code}] request failed!")
            return None

    def __fetch_and_save_data(self):
        table = self.__request_data()
        print(table)
        if table is None:
            print("Data not found")
            return

        data = dict()
        for i, row in enumerate(table.find_all('tr')):
            if i == 1:
                continue
            if i == 0:
                for j, col in enumerate(row.find_all('td')):
                    col = col.get_text()
                    data[col] = list()
            else:
                col_values = row.find_all('td')
                for j, col_names in enumerate(data.keys()):
                    value = col_values[j].get_text().replace('\n', '')
                    data[col_names].append(value)


        data = pd.DataFrame(data)

        print(data)

        data.to_csv(self.filename, header=True, index=False, sep='\t', mode='w')
        print(f"File saved in {self.filename}...")


if __name__ == "__main__":
    crawer = Crawler(station_id=857341, year=2014, month=10)
    crawer




import os
from string import Template
import requests
from bs4 import BeautifulSoup
import pandas as pd


class Crawler:
    def __init__(self, station_id, country, province, frequency, **kwargs):
        self.station_id = station_id
        self.frequency = frequency
        self.country = country
        self.province = province
        self.year = kwargs.get('year', None)
        self.month = kwargs.get('month', None)
        self.day = kwargs.get('day', None)
        self.path = os.path.dirname(os.path.realpath(__file__))
        self.args = dict(
            path=self.path,
            country=self.country,
            province=self.province,
            sid=self.station_id,
            ufreq=self.frequency.upper(),
            freq=self.frequency,
            year=self.year,
            month=self.month,
            day=self.day
        )

        if self.frequency == 'hourlyUrl':
            self.dirname = Template(
                "$path/data/$country/$province/$sid/$ufreq/$year/$month/$day"
            ).substitute(**self.args)
            self.args['dirname'] = self.dirname
            self.filename = Template(
                "$dirname/SID$sid-Y$year-M$month-D$day.txt"
            ).substitute(**self.args)
            self.__make_directories()
            self.base_url = "http://www.sasscalweathernet.org/weatherstat_hourly_we.php"
            self.args['burl'] = self.base_url
            self.params_url = Template(
                "$burl?year_crit=$year&graph_category=1&gesendet=Start+search&loggerid_crit=$sid"
            ).substitute(**self.args)
        elif self.frequency == 'dailyUrl':
            self.dirname = Template(
                "$path/data/$country/$province/$sid/$ufreq/$year/$month"
            ).substitute(**self.args)
            self.args['dirname'] = self.dirname
            self.filename = Template(
                "$dirname/SID$sid-Y$year-M$month.txt"
            ).substitute(**self.args)
            self.__make_directories()
        elif self.frequency == 'monthlyUrl':
            self.dirname = Template(
                "$path/data/$country/$province/$sid/$ufreq/$year"
            ).substitute(**self.args)
            self.args['dirname'] = self.dirname
            self.filename = Template(
                "$dirname/SID$sid-Y$year.txt"
            ).substitute(**self.args)
            self.__make_directories()

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

            )
            return table
        else:
            print(f"[{res.status_code}] request failed!")

    def download_daily(self):
        self.base_url = "http://www.sasscalweathernet.org/weatherstat_daily_we.php"
        self.args['burl'] = self.base_url
        self.params_url = Template(
            "$burl?yrmth_crit=$year-$month&graph_category=1&gesendet=Start+search&loggerid_crit=$sid"
        ).substitute(**self.args)

        if os.path.exists(self.filename):
            print('File was exist...')
        else:
            selector = '#inb_1 > table'
            table = self.__request_data().select(selector)

            if len(table) == 0:
                print("Data not found")
                return
            table = table[0]
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

            data.to_csv(
                self.filename, header=True, index=False, sep='\t', mode='w'
            )
            print(f"File saved in {self.filename}...")

    def download_monthly(self):
        self.base_url = "http://www.sasscalweathernet.org/weatherstat_monthly_we.php"
        self.args['burl'] = self.base_url
        self.params_url = Template(
            "$burl?year_crit=$year&graph_category=1&gesendet=Start+search&loggerid_crit=$sid"
        ).substitute(**self.args)

        print(self.params_url)
        if os.path.exists(self.filename):
            print('File was exist...')
        else:
            selector = '#inb_1 > table'
            table = self.__request_data().select(selector)
            if len(table) == 0:
                print("Data not found")
                return
            table = table[1]

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

            data.to_csv(
                self.filename, header=True, index=False, sep='\t', mode='w'
            )
            print(f"File saved in {self.filename}...")

    def __download_hourly(self):
        self.payload = dict(
            date_crit=f"{self.year}-{self.month}",
            graph_category="1",
            gesendet="Start search",
            loggerid_crit=f"{self.station_id}"
        )


if __name__ == "__main__":
    frequency = 'monthlyUrl'
    country = 'South Africa'
    provinces = [
        'Alpha',
        'Vandersterrberg',
        'Yellow Dune - Grootderm',
        'Alexanderbay Lichen Field',
        'Eksteenfontein',
        'Moedverloor',
        'Verlorenvlei',
        'Eagles Pride',
        'Roelofsberg',
        'De-Poort',
    ]
    
    # provinces = [
    #     'Okangwati',
    #     'Okalongo',
    #     'Omafo',
    #     'Kalimbeza',
    #     'Sachinga',
    #     'Ogongo',
    #     'Bagani',
    #     'Hamoye',
    #     'Kaoko Otavi',
    #     'Alex Muranda Livestock Development Centre',
    #     'Okashana',
    #     'Okapya',
    #     'Giribesvlakte',
    #     'Mannheim',
    #     'Tsumkwe Breeding Station',
    #     'John Pandeni',
    #     'Khorixas',
    #     'Waterberg',
    #     'Omatjenne',
    #     'Okomumbonde',
    #     'Sandveld',
    #     'Wlotzkasbaken',
    #     'Windhoek (NBRI)',
    #     'Windhoek (Satellite)',
    #     'Claratal',
    #     'Marble Koppie',
    #     'Sophies Hoogte',
    #     'Coastal Met',
    #     'Vogelfederberg',
    #     'Garnet Koppie',
    #     'Narais - Duruchaus',
    #     'Station 8',
    #     'Aussinanis',
    #     'Gobabeb Met',
    #     'Tsumis',
    #     'Dieprivier (Namib Desert Lodge)',
    #     'Gellap Ost',
    #     'Aurus Mountain',
    #     'Karios (Gondwana Canyon Lodge)',
    # ]

    for province in provinces:
        input_args = f"./input_data/{country}/{province}/{frequency.upper()}"
        input_args = list(
            os.path.join(input_args, file) for file in os.listdir(input_args)
        )[0]

        if frequency == 'hourlyUrl':
            args = list()
            with open(input_args, 'r') as handle:
                lines = handle.read().strip().split('\n')
                for line in lines:
                    line = line.split('$')
                    args.append(
                        dict(
                            year=line[3],
                            month=line[4],
                            day=line[5],
                            country=line[0],
                            province=line[1],
                            station_id=line[2],
                            frequency='hourlyUrl'
                        )
                    )
        elif frequency == 'dailyUrl':
            args = list()
            with open(input_args, 'r') as handle:
                lines = handle.read().strip().split('\n')
                for line in lines:
                    line = line.split('$')
                    args.append(
                        dict(
                            year=line[3],
                            month=line[4],
                            country=line[0],
                            province=line[1],
                            station_id=line[2],
                            frequency='dailyUrl'
                        )
                    )
        elif frequency == 'monthlyUrl':
            args = list()
            with open(input_args, 'r') as handle:
                lines = handle.read().strip().split('\n')
                for line in lines:
                    line = line.split('$')
                    args.append(
                        dict(
                            year=line[3],
                            country=line[0],
                            province=line[1],
                            station_id=line[2],
                            frequency='monthlyUrl'
                        )
                    )

        for arg in args:
            print("Working in ", arg)
            crawler = Crawler(**arg)
            if frequency == 'hourlyUrl':
                print("Not implemented")
                pass
            elif frequency == 'dailyUrl':
                crawler.download_daily()
            elif frequency == 'monthlyUrl':
                crawler.download_monthly()
            else:
                raise Exception("Frequency invalid!")
import os
import requests
from bs4 import BeautifulSoup


def get_nested_urls():
    root_url = "http://sasscalweathernet.org"
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
                    infoUrl=f"{root_url}/{links[0]}",
                    hourlyUrl=f"{root_url}/{links[1]}",
                    dailyUrl=f"{root_url}/{links[2]}",
                    monthlyUrl=f"{root_url}/{links[3]}",
                    dataUrl=f"{root_url}/{links[4]}"
                )
                nested_urls[country][province] = sub_urls
        return nested_urls
    else:
        print(f"[{res.status_code}] request failed!")
        return None


def fetch_and_save_input_files(nested_urls, freq='dailyUrl'):
    for i, country in enumerate(nested_urls.keys()):
        for j, province in enumerate(nested_urls[country].keys()):
            path = os.path.dirname(os.path.realpath(__file__))
            path = os.path.join(
                path, f"input_data/{country}/{province}/{freq.upper()}"
            )
            if not os.path.exists(path):
                os.makedirs(path)
            nest_url = nested_urls[country][province][freq]

            res = requests.get(nest_url)
            print(f"Requesting {nest_url}", end=' ')
            if res.status_code == 200:
                print(f"[{res.status_code}] request successfully...")

                soup = BeautifulSoup(res.text, 'html.parser')
                options = soup.select('#formular_search')[0].find_all('option')
                sid = nest_url.split('=')[-1]
                params = ''

                for k, option in enumerate(options):
                    year, month = option.attrs['value'].split('-')
                    param = f'{sid} {year} {month}'
                    if k < len(options):
                        params += f"{param}\n"
                    else:
                        params += f"{param}"
            else:
                print(f"[{res.status_code}] request failed!")
            filename = os.path.join(path, f"{freq.upper()}-{sid}.in")
            with open(filename, 'w') as handle:
                handle.write(params)
            print(f"File for {freq} {province} - {country} saved in {filename}")


if __name__ == "__main__":
    nest_urls = get_nested_urls()

    fetch_and_save_input_files(nest_urls)

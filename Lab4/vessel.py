import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import time
import sys
import random

sys.stdout.reconfigure(line_buffering=True)

data_storage = {"Name": [], "IMO": [], "MMSI": [], "Type": []}
error_log = []
processed_ok = 0

http_client = requests.Session()
http_client.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'DNT': '1',
    'Upgrade-Insecure-Requests': '1',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'none',
    'Sec-Fetch-User': '?1',
    'Cache-Control': 'max-age=0',
    'Referer': 'https://www.vesselfinder.com/',
})

time.sleep(random.uniform(3, 7))
_ = http_client.get("https://www.vesselfinder.com/", timeout=30)
time.sleep(2)
time.sleep(random.uniform(2, 5))

def extract_ship_info(page_content):
    empty_indicator = page_content.find("div", class_="no-result-row")
    if empty_indicator:
        return True

    total_info = page_content.find("div", class_="pagination-totals")
    if not total_info:
        return True
    num_text = total_info.get_text(strip=True)
    num_match = re.search(r'\d+', num_text)
    ship_count = int(num_match.group()) if num_match else 0

    if ship_count == 1:
        ship_name_elem = page_content.find("div", class_="slna")
        ship_category_elem = page_content.find("div", class_="slty")
        detail_anchor = page_content.find("a", class_="ship-link")

        if ship_name_elem and ship_category_elem and detail_anchor:
            ship_name = ship_name_elem.get_text(strip=True)
            ship_category = ship_category_elem.get_text(strip=True)

            href_val = detail_anchor.get('href', '')
            imo_match = re.search(r'/details/(\d+)', href_val)
            if imo_match:
                ship_imo = imo_match.group(1)
                data_storage["Name"].append(ship_name)
                data_storage["IMO"].append(ship_imo)
                data_storage["Type"].append(ship_category)
                data_storage["MMSI"].append("")

                detail_url = f"https://www.vesselfinder.com/ru/vessels/details/{ship_imo}"
                try:
                    detail_resp = http_client.get(detail_url, timeout=60)
                    if detail_resp.status_code == 200:
                        detail_soup = BeautifulSoup(detail_resp.text, "html.parser")
                        fetch_mmsi(detail_soup)
                        return True
                    else:
                        return False
                except:
                    return False
    return True


def fetch_mmsi(detail_page):
    scripts = detail_page.find_all("script")
    for script_elem in scripts:
        if script_elem.string:
            mmsi_pattern = re.search(r"var MMSI=(\d+)", script_elem.string)
            if mmsi_pattern:
                mmsi_code = mmsi_pattern.group(1)
                data_storage["MMSI"][-1] = mmsi_code
                break


def handle_url(target_url):
    global processed_ok
    try:
        time.sleep(random.uniform(8, 15))
        resp = http_client.get(target_url, timeout=60)
        if resp.status_code != 200:
            error_log.append((target_url, resp.status_code))
            return False

        page_soup = BeautifulSoup(resp.text, "html.parser")
        if extract_ship_info(page_soup):
            processed_ok += 1
            return True
    except:
        error_log.append((target_url, "exception"))
    return False


link_data = pd.read_excel("Links.xlsx", sheet_name=0)
total_links = len(link_data)

counter = 1
for url_entry in link_data["Ссылка"]:
    handle_url(url_entry)

    if counter % 20 == 0:
        print(f"обработано {counter}/{total_links}", flush=True)

    counter += 1

result_table = pd.DataFrame(data_storage)
result_table.to_excel('result.xlsx', index=False)
print(f"обработано {total_links}/{total_links}", flush=True)
print(f"успехов: {processed_ok}, ошибок: {len(error_log)}", flush=True)
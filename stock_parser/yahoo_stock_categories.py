import os
from os.path import dirname, abspath
import sys
import requests 
import re
import os
from bs4 import BeautifulSoup
import logging
import time

DIR_PATH = dirname(abspath(__file__))
parent_dir = os.path.dirname(DIR_PATH)
sys.path.append(parent_dir)

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'yahoo_stock_categories.log'), filemode='a', format=FORMAT)
logger = logging.getLogger('yahoo_stock_logger')



from db_func import query_from_db, insert_to_db, bulk_insert_to_db
start = time.time()
### Define the global header for requests
headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}
stock_prefix = """https://tw.stock.yahoo.com/"""

url = """https://tw.stock.yahoo.com/h/kimosel.php?tse=1&cat=%A5b%BE%C9%C5%E9&form=menu&form_id=stock_id&form_name=stock_name&domain=0"""
r = requests.get(url, headers = headers)
web_content = r.text
soup = BeautifulSoup(web_content, "lxml")

for match in soup.find_all('td', attrs = {'class': "c3", 'height': "25", "align": "center"}):
    try:
        if match.attrs.get('rowspan', ''):
            continue
        if not match.get_text().strip():
            continue
        #print(match)
        for match_url in match.find_all('a', href=True):
            insert_to_db("INSERT IGNORE INTO news_db.yahoo_stock_categories (category_name, category_url) VALUES (%s, %s)", (match_url.text, stock_prefix + match_url['href']))
    except Exception as e:
        print(e)
        logger.error(e)

logger.info("Finish capture stock categories in {} seconds".format(time.time() - start))
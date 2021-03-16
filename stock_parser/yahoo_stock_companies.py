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
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'yahoo_stock_companies.log'), filemode='a', format=FORMAT)
logger = logging.getLogger('yahoo_stock_logger')

from db_func import query_from_db, bulk_insert_to_db
start = time.time()
stock_df = query_from_db("""SELECT stock_category_id ,category_name, category_url FROM news_db.yahoo_stock_categories WHERE valid = 1""")

### Define the global header for requests
headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}

res = []
for idx, (stock_category_id, _, category_url) in stock_df.iterrows():
   # print(stock_category_id, category_url)
    r = requests.get(category_url, headers = headers)
    r.encoding='big5-hkscs'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    
    for match in soup.find_all('td', attrs = {'height': "25", "width": "154"}):
        try:
            if match.attrs.get('rowspan', ''):
                continue
            if not match.get_text().strip():
                continue
            company_info = match.get_text().strip().split(maxsplit = 1)
            res.append((stock_category_id, company_info[0], company_info[1]))
        except Exception as e:
            logger.error(e)
            print(e)
    
bulk_insert_to_db("INSERT IGNORE INTO news_db.yahoo_stock_companies (stock_category_id, stock_ticker, company_name) VALUES (%s, %s, %s)", res)
logger.info("Finish extract yahoo stock companies in {} seconds".format(time.time() - start))

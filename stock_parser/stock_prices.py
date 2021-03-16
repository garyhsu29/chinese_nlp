import os
from os.path import dirname, abspath
import sys
import requests 
import logging
import time
from datetime import datetime

DIR_PATH = dirname(abspath(__file__))
parent_dir = os.path.dirname(DIR_PATH)
sys.path.append(parent_dir)

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'stock_prices.log'), filemode='a', format=FORMAT)
logger = logging.getLogger('stock_price_logger')

from db_func import query_from_db, bulk_insert_to_db, insert_to_db

start = time.time()
ticker_df = query_from_db("SELECT stock_ticker FROM news_db.yahoo_stock_companies")
#ticker_df = ticker_df.iloc[ticker_df[ticker_df['stock_ticker'] == '2891B'].index.values[0]:]
def date_convertor(date_text):
    temp_y, temp_m, temp_d = date_text.split('/')
    temp_y = str(int(temp_y) + 1911)
    temp = '/'.join([temp_y, temp_m, temp_d])
    return datetime.strptime(temp, '%Y/%m/%d')
#日期', '成交股數', '成交金額', '開盤價', '最高價', '最低價', '收盤價', '漲跌價差', '成交筆數'
#'data': [['107/01/02', '13,698,944', '499,370,945', '36.45', '36.60', '36.05', '36.55', '+0.10', '3,932']
def format_convertor(stock_ticker, data):
    cur_date, volumn, total_price, open_price, high_price, low_price, close_price, price_diff, transaction = data
    transaction = int(transaction.replace(',',''))
    volumn  = int(volumn.replace(',',''))
    total_price = int(total_price.replace(',',''))
    if open_price == '--':
        open_price = None
    else:
        open_price = float(open_price.replace(',',''))
    if high_price == '--':
        high_price = None
    else:
        high_price = float(high_price.replace(',',''))
    if low_price == '--':
        low_price = None
    else:
        low_price = float(low_price.replace(',',''))
    if close_price == '--':
        close_price = None
    else:
        close_price = float(close_price.replace(',',''))
    try:
        price_diff = float(price_diff.replace(',',''))
    except Exception as e:
        price_diff = 0.0
        print(e)
        logger.error(e)
    
    return (stock_ticker, date_convertor(cur_date), open_price, high_price, low_price, close_price, price_diff, volumn, total_price, transaction)

headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}
def get_data_from_twse(stock_ticker, date_info):
    current_url = """https://www.twse.com.tw/exchangeReport/STOCK_DAY?response=json&date={}01&stockNo={}""".format(date_info, stock_ticker)
    #print(current_url)
    r = requests.get(current_url, headers = headers)
    web_info = r.json()
    res = []
    
    #print(web_info)
    if web_info['stat'] == '很抱歉，沒有符合條件的資料!':
        logger.info(current_url)
        logger.info("UPDATE the check_flag of {} to 1".format(stock_ticker))
        insert_to_db("UPDATE news_db.yahoo_stock_companies SET check_flag = 1 WHERE stock_ticker = '{}'".format(stock_ticker))
    else:
        try:
            for data in web_info['data']:
                res.append(format_convertor(stock_ticker, data))
        except Exception as e:
            time.sleep(3)
            r = requests.get(current_url, headers = headers)
            web_info = r.json()
            res = []
            if web_info['stat'] == '很抱歉，沒有符合條件的資料!':
                logger.info(current_url)
                logger.info("UPDATE the check_flag of {} to 1".format(stock_ticker))
                insert_to_db("UPDATE news_db.yahoo_stock_companies SET check_flag = 1 WHERE stock_ticker = '{}'".format(stock_ticker))
            else:
                try:
                    for data in web_info['data']:
                        res.append(format_convertor(stock_ticker, data))
                except Exception as e:
                    print("Try two times error: ", e)
                    logger.error("Try two times error:\nstock_ticker: {}, Date Info: {}".format(stock_ticker, date_info))
                    return 0
    bulk_insert_to_db("INSERT IGNORE INTO news_db.stock_prices (stock_ticker, date, open, high, low, close, price_diff, volume, total_price, transaction) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", res)
    time.sleep(3)


cur_year, cur_month, cur_date = str(datetime.now().date()).split('-')
"""
search_range = []
for year in range(2018, int(cur_year)):
    for month in range(1, 13):
        search_range.append(str(year) + str(month).zfill(2))
for month in range(1, int(cur_month) + 1):
    search_range.append(str(cur_year) + str(month).zfill(2))

for index, (stock_ticker) in ticker_df.iterrows():
    for date_info in search_range:
        cur_stock_ticker = stock_ticker.values[0]
        get_data_from_twse(cur_stock_ticker, date_info)
"""
for index, (stock_ticker) in ticker_df.iterrows():
    cur_stock_ticker = stock_ticker.values[0]
    get_data_from_twse(cur_stock_ticker, str(cur_year) + str(cur_month).zfill(2))
logger.info("Finish insert all stock_prices in {} seconds".format(time.time() - start))
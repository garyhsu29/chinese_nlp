import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
from content_parser import ContentParser
import html

start = time.time()
requests.adapters.DEFAULT_RETRIES = 5 

import re

def pchome_content_processor(url):
    res_dict = {}
    r = requests.get(url, headers = content_parser.headers)
    r.encoding='utf-8'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    if not soup:
        return
    for br in soup.find_all("br"):
        br.replace_with("\n")
    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.rsplit('-', 2)
        res_dict['news_title'] = html.unescape(title_category[0].strip())
        if len(title_category) > 1:
            res_dict['news_category'] = html.unescape(title_category[-2].strip())
            
    fb_app_tag = soup.find('meta', attrs = {'property':'fb:app_id'})
    if fb_app_tag:
        res_dict['news_fb_app_id'] = str(fb_app_tag['content'])
    
    fb_page_tag = soup.find('meta', attrs = {'property':'fb:pages'})
    if fb_page_tag:
        res_dict['news_fb_page'] = str(fb_page_tag['content'])

    #Optional
    keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
    if keywords_tag:
        res_dict['news_keywords'] = html.unescape(keywords_tag['content'])

    description_tag = soup.find('meta', attrs = {'name': 'description'})
    if description_tag:
        res_dict['news_description'] = html.unescape(description_tag['content'])

    time_tag = soup.find('time')

    if time_tag:
        try:
            d1 = datetime.datetime.strptime(time_tag.get('pubdate'), "%Y-%m-%d %H:%M:%S") 
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag.get('pubdate'), "%Y-%m-%d %H:%M:%SZ") 
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                print(e2)
                content_parser.logger.info('PChome date error {}, URL: {}'.format(e2, url))

    article_body_tag = soup.find('div', attrs = {'calss':'article_text'})
    if article_body_tag:
        content = article_body_tag.text.strip()
        a_tags = article_body_tag.find_all('a')
        if content:
            content = re.sub('(\n)+', '\n', html.unescape(content))
            content = re.sub(r'(相關新聞[\s\S]+)', '', content)
            res_dict['news'] = html.unescape(content)
            
    if not res_dict or 'news' not in res_dict:
        content_parser.logger.error('PChome url: {} did not process properly'.format(url))
        return
        
    return res_dict
content_parser = ContentParser('PCHOME')
# Query the data with source name
unprocessed_data = content_parser.content_query()
content_parser.content_processor(unprocessed_data, pchome_content_processor)
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed PChome {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))

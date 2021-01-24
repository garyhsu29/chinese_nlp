import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
import re
from content_parser import ContentParser

start = time.time()
requests.adapters.DEFAULT_RETRIES = 5 



def msn_content_processor(url):
    res_dict = {}
    prefix = ['相關報導', '※', '＊', '更多三立新聞網報導', '延伸閱讀', '資料來源', '更多中時電子報精彩報導', '看了這篇文章的人，也']
    pattern = re.compile(r'^{}'.format('|'.join(prefix)))
    
    if url.startswith('https://www.msn.com/zh-tw/video/'):
        return
    r = requests.get(url, headers = content_parser.headers)
    r.encoding='utf-8'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")

    if not soup:
        return 
    title_tag = soup.find("title")
    if title_tag:
        res_dict['news_title'] = title_tag.text.strip()
        
    
    category_tag = soup.find('div', attrs = {'class':'logowrapper'})
    if category_tag:
        _, category = category_tag.get_text().strip().split('\n')
        res_dict['news_category'] = category
        
    description_tag = soup.find('meta', attrs = {'name': 'description'})
    if description_tag:
        res_dict['news_description'] = description_tag['content']

    time_tag = soup.find('div', attrs = {'class': 'timeinfo-txt'})
    if time_tag:
        date_time_tag = time_tag.find('time')
        try:
            d1 = datetime.datetime.strptime(date_time_tag['datetime'], "%Y-%m-%dT%H:%M:%S.000Z") 
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(date_time_tag['datetime'], "%Y-%m-%dT%H:%M:%SZ") 
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                content_parser.logger.info('MSN date error, url: {}'.format(url))
    
    article_body_tag = soup.find('section', attrs = {'itemprop':'articleBody'})
    temp_content, links, links_descs = [], [], []
    if article_body_tag:
        p_tags = article_body_tag.find_all('p')
        div_tags = article_body_tag.find_all('div')
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            for p in p_tags:
                #print(p.get_text())
                # Ignore the image caption
                if p.find('span'):
                    p.span.decompose()
                if p.find('a'):
                    p.a.decompose()
                if p.text.strip():
                    if pattern.match(p.text.strip()):
                        break
                    elif p.text.strip()[0] in ('▲','▼'):
                        continue
                    temp_content.append(p.text.strip())
        elif div_tags:
            for div in div_tags:
                if div.find('span'):
                    div.span.decompose()
                if div.find('a'):
                    div.a.decompose()
                if div.text.strip():
                    if pattern.match(div.text.strip()):
                        break
                    elif div.text.strip()[0] in ('▲','▼'):
                        continue
                    temp_content.append(div.text.strip())
            
        if len(a_tags):
            for a in a_tags:
                if len(a):
                    if a['href'] == '#':
                        continue
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(a.get_text().strip())
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs

    if len(temp_content):
        content = '\n'.join(temp_content)
        res_dict['news'] = content

    if not res_dict or 'news' not in res_dict: 
        content_parser.logger.error('MSN url: {} did not process properly'.format(url))
        #print('MSN url: {} did not process properly'.format(url))
        return 
    return res_dict
content_parser = ContentParser('MSN')
# Query the data with source name
unprocessed_data = content_parser.content_query()

content_parser.content_processor(unprocessed_data, msn_content_processor)
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed MSN {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))

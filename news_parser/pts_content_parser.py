import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
import html
from content_parser import ContentParser

start = time.time()
requests.adapters.DEFAULT_RETRIES = 5 

def pts_content_processor(rss_id, url):
    res_dict = {}
    r = requests.get(url, headers = content_parser.headers)
    r.encoding='utf-8'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")

    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.rsplit(' | ', 1)
        res_dict['news_title'] = html.unescape(title_category[0].strip())
            
    fb_app_tag = soup.find('meta', attrs = {'property':'fb:app_id'})
    if fb_app_tag:
        res_dict['news_fb_app_id'] = str(fb_app_tag['content'])
    

    #Optional
    keywords_tag = soup.find('meta', attrs={'name': 'keywords'})
    if keywords_tag:
        res_dict['news_keywords'] = html.unescape(keywords_tag['content'])

    description_tags = soup.find_all('meta', attrs = {'name': 'description'})
    if len(description_tags) > 1:
        res_dict['news_description'] = html.unescape(description_tags[1]['content'])
        
    time_tag_2 = soup.find('div', attrs = {'class': 'maintype-wapper hidden-lg hidden-md'})
    time_tag = soup.find('time', attrs = {'class': None})
    if time_tag:
        try:
            d1 = datetime.datetime.strptime(time_tag.get_text(), "%Y-%m-%d %H:%M") 
            d2 = d1.date()
            d1 -= datetime.timedelta(hours=8)
            db_time_format = '%Y-%m-%d %H:%M:%S'
            time_res = d1.strftime(db_time_format)
            res_dict['published_time'] = time_res
            
            db_date_format = '%Y-%m-%d'
            date_res = d2.strftime(db_date_format)
            res_dict['published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag.get_text(), "%Y-%m-%d %H:%M:%S") 
                d2 = d1.date()
                d1 -= datetime.timedelta(hours=8)
                db_time_format = '%Y-%m-%d %H:%M:%S'
                time_res = d1.strftime(db_time_format)
                res_dict['published_time'] = time_res
            
                db_date_format = '%Y-%m-%d'
                date_res = d2.strftime(db_date_format)
                res_dict['published_date'] = date_res
            except Exception as e2:
                content_parser.logger.info('PTS date error {}, URL: {}'.format(e2, url))
    elif time_tag_2:
        date_tag = time_tag.find('h2')
        if date_tag:
            try:
                d1 = datetime.datetime.strptime(date_tag.text, "%Y年%m月%d日")
                d2 = d1 + datetime.timedelta(hours=8)
                d2 = d2.date()
                #d1 -= datetime.timedelta(hours=8)
                db_time_format = '%Y-%m-%d'
                time_res = d1.strftime(db_time_format)
                res_dict['published_time'] = time_res
                
                db_date_format = '%Y-%m-%d'
                date_res = d2.strftime(db_date_format)
                res_dict['published_date'] = date_res
            except Exception as e1:
                print(e1)
                content_parser.logger.info('PTS date error {}, URL: {}'.format(e1, url))
    

    article_body_tag = soup.find('div', attrs = {'class':'article_content'})
    article_body_tag_2 = soup.find('article', attrs = {'class': 'post-article'})
    if article_body_tag:
        content = article_body_tag.text.strip()
        if content:
            res_dict['news'] = html.unescape(content)
    elif article_body_tag_2:
        content = article_body_tag_2.text.strip()
        if content:
            res_dict['news'] = html.unescape(content)
    
            
    if not res_dict or 'news' not in res_dict:
        content_parser.logger.error('PTS url: {} did not process properly'.format(url))
        content_parser.errors['process_empty_content_(rss_id)'].append([rss_id, url])
        return

    return res_dict
content_parser = ContentParser('公視新聞網')
# Query the data with source name
unprocessed_data = content_parser.content_query()

content_parser.content_processor(unprocessed_data, pts_content_processor)
if content_parser.errors:
    content_parser.sent_error_email()
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed PTS {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))

import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
from content_parser import ContentParser

start = time.time()

requests.adapters.DEFAULT_RETRIES = 5 

def epoch_content_processor(url):
    res_dict = {}
    r = requests.get(url, headers = content_parser.headers)
    r.encoding='utf-8'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    if not soup:
        return 
    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.split(' | ', 1)
        res_dict['news_title'] = title_category[0]
        
    
    category_tag = soup.find('meta', attrs = {'property':'article:section'})
    if category_tag:
        res_dict['news_category'] = str(category_tag['content'])
        
        
    fb_app_tag = soup.find('meta', attrs = {'property':'fb:app_id'})
    if fb_app_tag:
        res_dict['news_fb_app_id'] = str(fb_app_tag['content'])

    fb_page_tag = soup.find('meta', attrs = {'property':'fb:pages'})
    if fb_page_tag:
        res_dict['news_fb_page'] = str(fb_page_tag['content'])

    #Optional
    keywords_tag = soup.find('meta', attrs={'name': 'news_keywords'})
    if keywords_tag:
        res_dict['news_keywords'] = keywords_tag['content']

    description_tag = soup.find('meta', attrs = {'name': 'description'})
    if description_tag:
        res_dict['news_description'] = description_tag['content']

    time_tag = soup.find('meta', attrs = {'property': 'article:published_time'})

    if time_tag:
        try:
            d1 = datetime.datetime.strptime(time_tag.get('content'), "%Y-%m-%dT%H:%M:%S+08:00") 
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag.get('content'), "%Y-%m-%dT%H:%M:%SZ") 
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                print(e2)
                content_parser.logger.info('Epoch date error {}'.format(e2))

    article_body_tag = soup.find('div', attrs = {'itemprop':'articleBody'})
    #print(article_body_tag)
    temp_content = []
    links = []
    links_descs = []
    if article_body_tag:
        p_tags = article_body_tag.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            
            for p in p_tags:
                if p.find('script', attrs = {'type': 'text/javascript'}):
                    continue
                temp_content.append(p.get_text().strip())
                
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
    content = '\n'.join(temp_content).strip()
    if content:
        res_dict['news'] = content

    if not res_dict or 'news' not in res_dict:
        content_parser.logger.error('Epoch url: {} did not process properly'.format(url))
        return
    return res_dict



content_parser = ContentParser('大紀元')
# Query the data with source name
unprocessed_data = content_parser.content_query()

content_parser.content_processor(unprocessed_data, epoch_content_processor)
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed Epoch {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))

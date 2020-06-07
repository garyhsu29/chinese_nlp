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

def yahoo_content_processor(url):
    res_dict = {}

    r = requests.get(url, headers = content_parser.headers)
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.split(' - ')
        res_dict['news_title'] = title_category[0]
        if len(title_category) > 1:
            res_dict['news_category'] = title_category[1]
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

    time_tag = soup.find('time')
    if time_tag:
        try:
            d1 = datetime.datetime.strptime(time_tag['datetime'], "%Y-%m-%dT%H:%M:%S.%fZ")
            db_date_format = '%Y-%m-%d %H:%M:%S'
            res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = res
        except Exception as e:
            logging.error(e)
            print(e)
            pass

    article_body_tag = soup.find('article', attrs = {'itemprop':'articleBody'})
    temp_content = []
    links = []
    links_descs = []
    if article_body_tag:
        p_tags = article_body_tag.find_all('p')
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            for index, p in enumerate(p_tags):
                if p.get('content'):
                    temp_content.append(p.get_text().strip())
        if a_tags:
            for a in a_tags:
                if a.get_text().strip():
                    links.append(a['href'])
                    links_descs.append(a.get_text().strip())
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs

    if not temp_content:
        content_parser.logger.error('Yahoo url: {} did not process properly'.format(url))
        return
    else:    
        # Capture the description start with 公告 to content
        if title_category[0][:4] == '【公告】':
            prefix = title_category[0]
        else:
            prefix = ''
        content = prefix + ' '.join(temp_content).replace('。 ', '。\n')
        res_dict['news'] = content
        return res_dict




content_parser = ContentParser('Yahoo!奇摩股市')
# Query the data with source name
unprocessed_data = content_parser.content_query()

content_parser.content_processor(unprocessed_data, yahoo_content_processor)
content_parser.encoding_cursor.close()
content_parser.mydb.close()

content_parser.logger.info("Processed Yahoo {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))


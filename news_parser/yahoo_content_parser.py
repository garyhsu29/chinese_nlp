import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
from content_parser import ContentParser
import html
import random

requests.adapters.DEFAULT_RETRIES = 5 

def yahoo_content_processor(rss_id, url):
    res_dict = {}
    r = requests.get(url, headers = content_parser_1.headers)
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    for br in soup.find_all("br"):
        br.replace_with("\n")
    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.split(' - ')
        res_dict['news_title'] = html.unescape(title_category[0])
        if len(title_category) > 1:
            res_dict['news_category'] = html.unescape(title_category[1])
    fb_app_tag = soup.find('meta', attrs = {'property':'fb:app_id'})
    if fb_app_tag:
        res_dict['news_fb_app_id'] = str(fb_app_tag['content'])
    fb_page_tag = soup.find('meta', attrs = {'property':'fb:pages'})
    if fb_page_tag:
        res_dict['news_fb_page'] = str(fb_page_tag['content'])

    #Optional
    keywords_tag = soup.find('meta', attrs={'name': 'news_keywords'})
    if keywords_tag:
        res_dict['news_keywords'] = html.unescape(keywords_tag['content'])

    description_tag = soup.find('meta', attrs = {'name': 'description'})
    if description_tag:
        res_dict['news_description'] = html.unescape(description_tag['content'])

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
    caas_body_tag = soup.find('div', attrs = {'class': 'caas-body'})
    temp_content = []
    links = []
    links_descs = []
    if article_body_tag:
        p_tags = article_body_tag.find_all('p')
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            for index, p in enumerate(p_tags):
                if p.get('content'):
                    temp_content.append(html.unescape(p.get_text().strip()))
        if a_tags:
            for a in a_tags:
                if a.get_text().strip():
                    links.append(a['href'])
                    links_descs.append(html.unescape(a.get_text().strip()))
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs
    elif caas_body_tag:
        p_tags = caas_body_tag.find_all('p')
        a_tags = caas_body_tag.find_all('a')
        if p_tags:
            for index, p in enumerate(p_tags):
                temp_content.append(html.unescape(p.get_text()).strip())
        if a_tags:
            for a in a_tags:
                if a.get_text().strip():
                    links.append(a['href'])
                    links_descs.append(html.unescape(a.get_text().strip()))
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs
    if temp_content:    
        # Capture the description start with 公告 to content
        if title_category[0][:4] == '【公告】':
            prefix = title_category[0]
        else:
            prefix = ''
        content = prefix + '\n'.join(temp_content)#.replace('。 ', '。\n')
        res_dict['news'] = html.unescape(content)
        return res_dict
    else:
        content_parser_1.logger.error('Yahoo url: {} did not process properly'.format(url))
        content_parser.errors['process_empty_content_(rss_id)'].append([rss_id, url])
        return




start = time.time()
content_parser_1 = ContentParser('Yahoo Source 1')
unprocessed_data_1 = content_parser_1.content_query()
content_parser_1.content_processor(unprocessed_data_1, yahoo_content_processor)
if content_parser_1.errors:
    content_parser_1.sent_error_email()
content_parser_1.encoding_cursor.close()
content_parser_1.mydb.close()
content_parser_1.logger.info("Processed Yahoo Source 1 {} examples in {} seconds".format(len(unprocessed_data_1), time.time() - start))


start = time.time()
content_parser_2 = ContentParser('Yahoo奇摩新聞')
unprocessed_data_2 = content_parser_2.content_query()
content_parser_2.content_processor(unprocessed_data_2, yahoo_content_processor)
if content_parser_2.errors:
    content_parser_2.sent_error_email()
content_parser_2.encoding_cursor.close()
content_parser_2.mydb.close()
content_parser_2.logger.info("Processed Yahoo News {} examples in {} seconds".format(len(unprocessed_data_2), time.time() - start))


start = time.time()
content_parser_3 = ContentParser('Yahoo奇摩股市')
unprocessed_data_3 = content_parser_3.content_query()
content_parser_3.content_processor(unprocessed_data_3, yahoo_content_processor)
if content_parser_3.errors:
    content_parser_3.sent_error_email()
content_parser_3.encoding_cursor.close()
content_parser_3.mydb.close()
content_parser_3.logger.info("Processed Yahoo Stock {} examples in {} seconds".format(len(unprocessed_data_3), time.time() - start))






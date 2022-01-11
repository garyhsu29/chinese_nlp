import logging
import mysql.connector
import os
import requests
from bs4 import BeautifulSoup
import datetime
import time
from content_parser import ContentParser
import re
import html
start = time.time()

requests.adapters.DEFAULT_RETRIES = 5 


def udn_content_processor(rss_id, url):
    res_dict = {}
    r = requests.get(url, headers = content_parser.headers)
    r.encoding='utf-8'
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    if not soup:
        return 
    title_tag = soup.find("title")
    if title_tag:
        title_category_lst = title_tag.string.split(' | ', 3)
        res_dict['news_title'] = html.unescape(title_category_lst[0])
        try:
            res_dict['news_category'] = html.unescape(title_category_lst[2])
        except:
            pass
        
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

    time_tag2 = soup.find('div', attrs = {'class': 'shareBar__info--author'})
    time_tag  = soup.find('meta', attrs = {'name':'date'})
    if time_tag:
        try:
            d1 = datetime.datetime.strptime(time_tag.get('content'), "%Y/%m/%d %H:%M:%S")
            d2 = d1.date()
            d1 -= datetime.timedelta(hours=8)
            db_time_format = '%Y-%m-%d %H:%M:%S'
            time_res = d1.strftime(db_time_format)
            res_dict['published_time'] = time_res
            
            db_date_format = '%Y-%m-%d'
            date_res = d2.strftime(db_date_format)
            res_dict['published_date'] = date_res 
        except Exception as e1:
            content_parser.logger.info('UDN date error {}\n{}'.format(e1,time_tag))
            print(e1)
            print(time_tag)
    elif time_tag2:
        try:
            d1 = datetime.datetime.strptime(time_tag2.find('span').text, "%Y-%m-%d %H:%M")
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
                d_temp = re.search('(\d{4}\-\d{2}\-\d{2}\s\d{2}\:\d{2})', time_tag2.text).group(0)
                d1 = datetime.datetime.strptime(d_temp, "%Y-%m-%d %H:%M") 
                d2 = d1.date()
                d1 -= datetime.timedelta(hours=8)
                db_time_format = '%Y-%m-%d %H:%M:%S'
                time_res = d1.strftime(db_time_format)
                res_dict['published_time'] = time_res
                
                db_date_format = '%Y-%m-%d'
                date_res = d2.strftime(db_date_format)
                res_dict['published_date'] = date_res
            except Exception as e2:
                try:
                    d1 = datetime.datetime.strptime(time_tag2.get('content'), "%Y-%m-%d %H:%M:%S") 
                    d2 = d1.date()
                    d1 -= datetime.timedelta(hours=8)
                    db_time_format = '%Y-%m-%d %H:%M:%S'
                    time_res = d1.strftime(db_time_format)
                    res_dict['published_time'] = time_res
                    
                    db_date_format = '%Y-%m-%d'
                    date_res = d2.strftime(db_date_format)
                    res_dict['published_date'] = date_res
                except Exception as e3:
                    print(e3)
                    content_parser.logger.info('UDN date error {}'.format(e3))
    article_body_tag = soup.find('div', attrs = {'id':'article_body'})
    article_body_tag2 = soup.find('section', attrs = {'id':'article_body'})
    content_temp, links, links_descs = [], [], []
    if article_body_tag:
        p_tags = article_body_tag.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            for p in p_tags:
                if p.get_text().strip() and p.get_text().strip() != 'facebook':
                    content_temp.append(html.unescape(p.get_text().strip()))
        if len(a_tags):
            for a in a_tags:
                if len(a):
                    if a['href'] == '#':
                        continue
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(html.unescape(a.get_text().strip()))
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs
    elif article_body_tag2:
        p_tags = article_body_tag2.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag2.find_all('a')
        if p_tags:
            for p in p_tags:
                if p.get_text().strip() and p.get_text().strip() != 'facebook':
                    content_temp.append(p.get_text().strip())
        if len(a_tags):
            for a in a_tags:
                if a.get('href') and a['href'] == '#':
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(a.get_text().strip())
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs  
    content = '\n'.join(content_temp).strip()
    if content:
        res_dict['news'] = html.unescape(content)

    if not res_dict or 'news' not in res_dict:
        content_parser.logger.error('udn url: {} did not process properly'.format(url))
        content_parser.errors['process_empty_content_(rss_id)'].append([rss_id, url])
        return
        

    return res_dict


content_parser = ContentParser('經濟日報')
# Query the data with source name
unprocessed_data = content_parser.content_query()

content_parser.content_processor(unprocessed_data, udn_content_processor)
if content_parser.errors:
    content_parser.sent_error_email()
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed UDN {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))

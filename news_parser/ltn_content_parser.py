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


def ltn_content_processor(url):
    postfix = ["""一手掌握經濟脈動
    點我訂閱自由財經Youtube頻道""", 
    """☆民眾如遇同居關係暴力情形，可撥打113保護專線，或向各地方政府家庭暴力防治中心求助☆""", 
    """☆健康新聞不漏接，按讚追蹤粉絲頁☆更多重要醫藥新聞訊息，請上自由健康網""",
    """《自由開講》是一個提供民眾對話的電子論壇，不論是對政治、經濟或社會、文化等新聞議題，有意見想表達、有話不吐不快，都歡迎你熱烈投稿。文長700字內為優，來稿請附真實姓名（必寫。有筆名請另註）、職業、聯絡電話、E-mail帳號。本報有錄取及刪修權，不付稿酬；請勿一稿多投，錄用與否將不另行通知。投稿信箱：LTNTALK@gmail.com""",
    """「武漢肺炎專區」請點此，更多相關訊息，帶您第一手掌握。""",
    """"""]
    res_dict = {}
    #headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}
    r = requests.get(url, headers = content_parser.headers)
    web_content = r.text
    soup = BeautifulSoup(web_content, "lxml")
    if not soup:
        return 
    title_tag = soup.find("title")
    if title_tag:
        title_category = title_tag.string.split(' - ')
        res_dict['news_title'] = html.unescape(title_category[0].strip())
        if len(title_category) > 1:
            res_dict['news_category'] = html.unescape(title_category[1].strip())
        else:
            try:
                res_dict['news_title'], res_dict['news_category'] = title_tag.string.split('|')
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

    time_tag_1 = soup.find('meta', attrs = {'property': 'article:published_time'})
    time_tag_2 = soup.find('meta', attrs = {'name': 'pubdate'})
    time_tag_3 = soup.find('span', attrs = {'class': 'time'})
    if time_tag_1:
        try:
            d1 = datetime.datetime.strptime(time_tag_1.get('content'), "%Y-%m-%dT%H:%M:%S+08:00") 
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag_1.get('content'), "%Y-%m-%dT%H:%M:%SZ") 
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                print('LTN date error {}, URL: {}'.format(e2, url))
                #content_parser.logger.info('LTN date error {}, URL: {}'.format(e2, url))
    elif time_tag_2:
        try:
            d1 = datetime.datetime.strptime(time_tag_2.get('content'), "%Y-%m-%dT%H:%M:%S+08:00")
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag_2.get('content'), "%Y-%m-%dT%H:%M:%SZ") 
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                print('LTN date error {}, URL: {}'.format(e2, url))
                #content_parser.logger.info('LTN date error: {}, URL:{}'.format(e2, url))
    elif time_tag_3:
        try:
            d1 = datetime.datetime.strptime(time_tag_3.get_text(), "%Y-%m-%d %H:%M")
            d1 -= datetime.timedelta(hours=8)
            db_date_format = '%Y-%m-%d %H:%M:%S'
            date_res = d1.strftime(db_date_format)
            res_dict['news_published_date'] = date_res
        except Exception as e1:
            try:
                d1 = datetime.datetime.strptime(time_tag_3.get_text(), "%Y-%m-%d %H:%M:%S")
                d1 -= datetime.timedelta(hours=8)
                db_date_format = '%Y-%m-%d %H:%M:%S'
                date_res = d1.strftime(db_date_format)
                res_dict['news_published_date'] = date_res
            except Exception as e2:
                print('LTN date error {}, URL: {}'.format(e2, url))
                #content_parser.logger.info('LTN date error: {}, URL:{}'.format(e2, url))
                

    article_body_tag = soup.find('div', attrs = {'itemprop':'articleBody'})
    article_body_tag2 = soup.find('div', attrs = {'class':'text boxTitle boxText'})
    article_body_tag3 = soup.find('div', attrs = {'class':'text'})
    temp_content = []
    links = []
    links_descs = []

    if article_body_tag:
        p_tags = article_body_tag.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag.find_all('a')
        if p_tags:
            for p in p_tags:
                if p.find('span'):
                    p.span.decompose()
                if p.text:
                    if p.text.strip() not in postfix :
                        temp_content.append(html.unescape(p.text.strip()))
                    else:
                        break
        if len(a_tags):
            for a in a_tags:
                if len(a):
                    if a.get('href', '')  == '#':
                        continue
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(a.get_text().strip())
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs
    elif article_body_tag2:
        p_tags = article_body_tag2.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag2.find_all('a')
        if p_tags:
            temp = []
            for p in p_tags:
                if p.find('span'):
                    p.span.decompose()
                if p.text:
                    if p.text.strip() not in postfix :
                        temp_content.append(html.unescape(p.text.strip()))
                    else:
                        break
        if len(a_tags):
            for a in a_tags:
                if len(a):
                    if a.get('href', '') == '#':
                        continue
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(a.get_text().strip())
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs
    elif article_body_tag3:
        p_tags = article_body_tag3.find_all('p', attrs = {'class': None})
        a_tags = article_body_tag3.find_all('a')
        if p_tags:
            temp = []
            for p in p_tags:
                if p.find('span'):
                    p.span.decompose()
                if p.text:
                    if p.text.strip() not in postfix:
                        temp_content.append(html.unescape(p.text.strip()))
                    else:
                        break
        if len(a_tags):
            for a in a_tags:
                if len(a):
                    if a.get('href', '') == '#':
                        continue
                    if a.get_text().strip() and 'www' in a['href']:
                        links.append(a['href'])
                        links_descs.append(html.unescape(a.get_text().strip()))
            res_dict['news_related_url'] = links
            res_dict['news_related_url_desc'] = links_descs

    content = '\n'.join(temp_content).strip()
    if content:
        res_dict['news'] = html.unescape(content)

    if not res_dict or 'news' not in res_dict:
        #print('LTN {}, did not process properly'.format(url))
        content_parser.logger.error('LTN url: {} did not process properly'.format(url))
        return
    return res_dict

# Initialize the Parser
content_parser = ContentParser('自由時報')
# Query the data with source name
unprocessed_data = content_parser.content_query()
content_parser.content_processor(unprocessed_data, ltn_content_processor)
content_parser.encoding_cursor.close()
content_parser.mydb.close()
content_parser.logger.info("Processed LTN {} examples in {} seconds".format(len(unprocessed_data), time.time() - start))
#mydb.close()
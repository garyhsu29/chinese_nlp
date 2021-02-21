import mysql.connector
import logging
import os
from os.path import dirname, abspath
import time
import datetime
import pickle
import sys 
from collections import defaultdict
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import smtplib

start = time.time()


DIR_PATH = dirname(abspath(__file__))
parent_path = dirname(dirname(abspath(__file__)))
with open(os.path.join(parent_path, 'configs', 'server2server.config'), 'rb') as f:
    configs = pickle.load(f)
with open(os.path.join(parent_path, 'configs', 'email_pw.config'), 'rb') as f:
    email_info = pickle.load(f)
#logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'content_parser.log'), filemode='a', format=FORMAT)


class ContentParser(object):
    def __init__(self, source_name, process_count = 200):
        self.headers = {'user-agent': 'Mozilla/5.0 (Macintosh Intel Mac OS X 10_13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36',  'Connection': 'close'}
        self.process_count = process_count
        self.mydb = mysql.connector.connect(
            host = configs['host'],
            user =  configs['user'],
            passwd = configs['passwd'],
            database = configs['database'],
            charset='utf8')
        # Create a cursor.
        self.encoding_cursor = self.mydb.cursor()

        # Enforce UTF-8 for the connection.
        self.encoding_cursor.execute('SET NAMES utf8mb4')
        self.encoding_cursor.execute("SET CHARACTER SET utf8mb4")
        self.encoding_cursor.execute("SET character_set_connection=utf8mb4")

        #self.mydb.set_charset_collation('utf8mb4')
        self.source_name = source_name
        self.logger = logging.getLogger('__name__')
        self.logger.setLevel(logging.INFO)
        fileHandle = logging.FileHandler(os.path.join(DIR_PATH, 'logs/{}.log'.format(source_name)))
       
        formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
        fileHandle.setFormatter(formatter)
        self.logger.addHandler(fileHandle)

        self.email_info = email_info
        self.email = MIMEMultipart()  #建立MIMEMultipart物件
        self.email["subject"] = "Parsing Result of {}, {}".format(source_name, datetime.datetime.now())  #郵件標題
        self.email["from"] = email_info['addr']  #寄件者
        self.email["to"] = email_info['addr'] #收件者
        self.errors = defaultdict(list)



    def content_query(self):
        mycursor = self.mydb.cursor(buffered = True)
        # rss_source LIKE '%Yahoo!奇摩股市%'rss_source LIKE %s ORDER BY created_time DESC
        mycursor.execute("SELECT news_rss_feeds_id, news_source, news_url FROM news_rss_feeds WHERE processed_status = 0 AND processed_success = 0  AND news_source = %s LIMIT %s", (self.source_name, self.process_count))
        myresult = mycursor.fetchall()
        #print(myresult)
        mycursor.close()
        return myresult
    
    
    def content_insert(self, rss_id ,res_dict):
        # Update the rss_table to mark process_status as 1
        myresult = 0
        mycursor = self.mydb.cursor()
        try:
            placeholders = ', '.join(['%s'] * len(res_dict))
            columns = ', '.join(res_dict.keys())
            sql = "INSERT INTO news_contents ({}) VALUES ({})".format(columns, placeholders)
            mycursor.execute(sql, list(res_dict.values()))
        except Exception as e:
            self.logger.error('Content insert error: {}'.format(e))
            #self.errors['duplicate_entry_(rss_id)'].append(rss_id)
            #sprint(e)
        else:
            self.mydb.commit()
            myresult = mycursor.lastrowid
            mycursor.close()
        finally:
            try:
                update_cursor = self.mydb.cursor()
                sql = "UPDATE news_rss_feeds SET processed_success = 1 WHERE news_rss_feeds_id = {}".format(str(rss_id))
                update_cursor.execute(sql)
            except Exception as e:
                self.logger.error('Success tag insert error: {}'.format(e))
                self.errors['update_success_flag_fail_(rss_id)'].append(rss_id)

                print(e)
            else:
                self.mydb.commit()
            finally:
                update_cursor.close()

        return myresult

    def content_insert_urls(self, news_id, news_related_url, news_related_url_desc):
        url_insert_cursor = self.mydb.cursor()
        sql = "INSERT INTO news_related_urls (news_related_url, news_related_url_desc, news_id) VALUES (%s, %s, %s)"
        val = [(news_related_url, news_related_url_desc, news_id) for news_related_url, news_related_url_desc in zip(news_related_url, news_related_url_desc)]
        try:
            url_insert_cursor.executemany(sql, val)
        except Exception as e:
            self.logger.error('Related urls insert error: {}'.format(e))
            self.errors['related_urls_insert_fail_(news_id)'].append(news_id)
            print('Related urls insert error: {}'.format(e))
        else:
            self.mydb.commit()
        url_insert_cursor.close()


    def content_processor(self, unprocessed_data, processor_func):
        mycursor = self.mydb.cursor()
        for rss_id, rss_source, url in unprocessed_data:
            try:
                sql = "UPDATE news_rss_feeds SET processed_status = 1 WHERE news_rss_feeds_id = {}".format(str(rss_id))
                mycursor.execute(sql)
            except Exception as e:
                self.logger.error(e)
                self.errors['update_status_flag_fail_(rss_id)'].append(rss_id)
                print(e)
            else:
                self.mydb.commit()
            res_dict = {}
            res_dict = processor_func(rss_id, url)
            ## Already put loggings in the file of its parser
            if not res_dict or 'news' not in res_dict:
                continue

            news_related_url = res_dict.pop('news_related_url', None)
            news_related_url_desc = res_dict.pop('news_related_url_desc', None)
            

            #res_dict['news_url'] = url
            res_dict['news_rss_feeds_id'] = rss_id
            # If the ltn did not process the 
            content_id = self.content_insert(rss_id, res_dict)
            if content_id and news_related_url and news_related_url_desc and len(news_related_url) == len(news_related_url_desc):
                self.content_insert_urls(content_id, news_related_url, news_related_url_desc)
        mycursor.close()

    def sent_error_email(self):
        with smtplib.SMTP(host="smtp.gmail.com", port="587") as smtp:  # 設定SMTP伺服器
            try:
                smtp.ehlo()  # 驗證SMTP伺服器
                smtp.starttls()  # 建立加密傳輸
                smtp.login(self.email_info['addr'], self.email_info['pw'])  # 登入寄件者gmail
                for reason, ids in self.errors.items():
                    self.email.attach(MIMEText("{}: {}\n".format(reason, ids)))
                smtp.send_message(self.email)  # 寄送郵件
            except Exception as e:
                self.logger.error("Error message: ", e)
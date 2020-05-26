import logging
import os 
import time
import re
import mysql.connector
from script_helper import query_from_db
import pickle

start = time.time()
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
PROCESS_NAME = 'sent_process'
FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'sent_splitter.log'), filemode='a', format=FORMAT)
raw_df = query_from_db("SELECT content_id, content FROM garysql.content WHERE content_id not in (SELECT content_id from garysql.content_processes WHERE process_name = \'sent_process\') LIMIT 100;")

with open('server_config_local.pkl', 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'])

def insert_sents(raw_df):
    for index, (content_id, content) in raw_df.iterrows():
        process_cursor = mydb.cursor()
        try:
            process_cursor.execute("INSERT INTO garysql.content_processes (content_id, process_name) VALUES (%s, %s)", (content_id, PROCESS_NAME))
            mydb.commit()
        except Exception as e:
            logging.error('Error: {}\nContent ID: {}'.format(e, content_id))
            mydb.rollback()
        process_cursor.close()

        sent_list = []
        for sent in re.split(r'。\s+|\n', content):
            if '延伸閱讀' in sent:
         	   break
            if len(sent):
                sent_list.append(sent + '。')


        sent_cursor = mydb.cursor()
        for sent_id, sent in enumerate(sent_list):
            try: 
                sent_cursor.execute("INSERT INTO garysql.content_sents (content_id, sent_id, sent) VALUES (%s, %s, %s)", (content_id, sent_id + 1, sent))
                mydb.commit()
            except Exception as e:
                logging.error('Error: {}\nSentence ID: {}'.format(e, sent_id))
                mydb.rollback()
        sent_cursor.close()


insert_sents(raw_df)
mydb.close()
logging.info('Finished predict {} example in {} seconds'.format(len(raw_df), time.time() - start))
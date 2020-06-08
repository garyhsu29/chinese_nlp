import logging
import os 
import time
import re
import mysql.connector
import pickle

from db_func import query_from_db

start = time.time()
DIR_PATH = os.path.dirname(os.path.realpath(__file__))

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'sent_splitter.log'), filemode='a', format=FORMAT)
raw_df = query_from_db("SELECT news_id, news FROM news_contents WHERE processed_status = 0 LIMIT 100;")

with open('server2server.config', 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'],
    database = configs['database']
    )

def insert_process(sql, val):
    process_cursor = mydb.cursor()
    try:
        process_cursor.execute(sql, val)
    except Exception as e:
        logging.error('Error: {}\nNews ID: {}'.format(e, news_id))
        mydb.rollback()
    else:
        mydb.commit()
    process_cursor.close()

def insert_sents(raw_df):
    for index, (news_id, content) in raw_df.iterrows():
        sql_status = "UPDATE news_contents SET processed_status = 1  WHERE news_id = %s"
        val_status = (news_id, )
        insert_process(sql_status, val_status)

        sent_list = []
        for sent in re.split(r'。\s+|\n', content):
            if '延伸閱讀' in sent:
         	   break
            if len(sent):
                sent_list.append(sent + '。')

        for sent_id, sent in enumerate(sent_list):
            sent_cursor = mydb.cursor()
            try: 
                sent_cursor.execute("INSERT INTO news_sents (news_id, sent_index, sent) VALUES (%s, %s, %s)", (news_id, sent_id + 1, sent))
                
            except Exception as e:
                logging.error('Error: {}\nSentence ID: {}'.format(e, sent_id))
                mydb.rollback()
            else:
                mydb.commit()
                sql_success = "UPDATE news_contents SET processed_success = 1  WHERE news_id = %s"
                val_success = (news_id, )
                insert_process(sql_success, val_success)

            sent_cursor.close()

insert_sents(raw_df)
mydb.close()
logging.info('Finished predict {} example in {} seconds'.format(len(raw_df), time.time() - start))
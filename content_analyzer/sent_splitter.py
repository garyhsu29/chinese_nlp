import logging
import os, sys
import time
import re
import mysql.connector
import pickle
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(parent_dir)

from db_func import query_from_db

start = time.time()
DIR_PATH = os.path.dirname(os.path.realpath(__file__))

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'sent_splitter.log'), filemode='a', format=FORMAT)
raw_df = query_from_db("SELECT news_id, news FROM news_contents WHERE processed_status = 0 LIMIT 300;")



with open(os.path.join(parent_dir, 'configs', 'server2server.config'), 'rb') as f:
    configs = pickle.load(f)

author_header = r'^((（.+?）)|(\(.+?\))|(【.+?】)|(〔.+?〕)|(\[.+?\])|(［.+?］))\s*'
content_footer = r'^更多.*?報導：?$|^更多新聞推薦|^【更多新聞】|^延伸閱讀：|^【延伸閱讀】|^超人氣$|^看更多.*?文章|^更多匯流新聞網報導：|^原始連結|^更多\w+內容：|^《TVBS》提醒您：|^※|^（延伸閱讀：|^相關影音：|^責任編輯：|^☆|^更多\w+相關新聞|►'


mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'],
    database = configs['database']
    )

def remove_header(author_header, header, sent):
    if not re.search(author_header, sent):
        return header, sent
    else:
        header_end = re.search(author_header, sent).end()
        return remove_header(author_header, header + sent[:header_end],sent[header_end:])
    
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

        author_list = []
        sent_list = []
        for index, sent in enumerate(re.split(r'\n', content)):
            if re.search(content_footer, sent):
                break
            if index == 0 and len(sent) < 20 and not re.search('。', sent):
                author_list.append(sent)
                continue
            if index <= 1 and re.search(author_header, sent):
                ori_sent = sent
                header, sent = remove_header(author_header, '',  ori_sent)
                author_list.append(header)
            if len(sent) > 1:
                sent_list.append(sent)

        for sent_id, sent in enumerate(sent_list):
            sent_cursor = mydb.cursor()
            try: 
                sent_cursor.execute("INSERT INTO news_sents (news_id, sent_index, sent) VALUES (%s, %s, %s)", (news_id, sent_id + 1, sent))
            except Exception as e:
                logging.error('Error: {}\nSentence ID: {}\nNews_id ID: {}'.format(e, sent_id, news_id))
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
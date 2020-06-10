from ckiptagger import WS, POS, NER
from db_func import query_from_db
import ast
import pickle
import logging
import mysql.connector
import time
start = time.time()
ws = WS("./data")
pos = POS("./data")
ner = NER("./data")


logging.basicConfig(format='%(asctime)s: %(levelname)s: %(message)s')
logging.root.setLevel(level=logging.INFO)

with open('server2server.config', 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'],
    database = configs['database'])

def insert_sentlevel_info(word_sentence, pos_sentence, news_sent_id):
    assert len(word_sentence) == len(pos_sentence)
    for word_index, (word, word_pos) in enumerate(zip(word_sentence, pos_sentence)):
        word_cursor = mydb.cursor()
        try:
            word_cursor.execute("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos) VALUES (%s, %s, %s, %s)", (news_sent_id, word_index, word, word_pos))
        except Exception as e:
            logging.error('Insert word pos error: {}\n News Sent ID: {}'.format(e, news_sent_id))
            mydb.rollback()
        else:
            mydb.commit()
        word_cursor.close()
        
def insert_ner_info(entity_sent_list, news_sent_id):
    for ent_id, (start_index, end_index, ent_type, ent_text) in enumerate(sorted(entity_sent_list, key = lambda x:x[0])):
        ner_cursor = mydb.cursor()
        try:
            ner_cursor.execute("INSERT INTO news_db.news_ners (news_sent_id, start_index, end_index, ent_type, ent_text) VALUES (%s, %s, %s, %s, %s)", (news_sent_id, start_index, end_index, ent_type, ent_text))
        except Exception as e:
            logging.error('Insert NER error: {}\n News Sent ID: {}'.format(e, news_sent_id))
            mydb.rollback()
        else:
            mydb.commit()
        ner_cursor.close()
        
def insert_process_flag(news_sent_id, process_name):
    process_cursor = mydb.cursor()
    try:
        process_cursor.execute("INSERT INTO news_db.nlp_processes (news_sent_id, process_name) VALUES (%s, %s)", (news_sent_id, process_name))
    except Exception as e:
        logging.error('Process Insert Error: {}\nNews sent ID: {}'.format(e, news_sent_id))
        mydb.rollback()
    else:
        mydb.commit()
    process_cursor.close()

def sent_level_analysis(raw_df):
    for index, (content_sent_id, sent) in raw_df.iterrows():
        insert_process_flag(content_sent_id, 'sent-analysis')
        try:
            word_sentence_list = ws([sent])
            pos_sentence_list = pos(word_sentence_list)
            entity_sentence_list = ner(word_sentence_list, pos_sentence_list)
        except Exception as e:
            logging.error('NLP process Error: {}\n Content ID: {}'.format(e, content_sent_id))
        
        
        insert_sentlevel_info(word_sentence_list[0],  pos_sentence_list[0], content_sent_id)
        insert_ner_info(entity_sentence_list[0], content_sent_id)
     

raw_df = query_from_db("SELECT nns.news_sent_id, nns.sent FROM news_db.news_sents nns WHERE nns.news_sent_id not in (SELECT nlp.news_sent_id FROM news_db.nlp_processes nlp WHERE nlp.process_name = \'sent-analysis\') LIMIT 1000")
sent_level_analysis(raw_df)
mydb.close()
logging.error('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))

import os, sys
from ckip_transformers.nlp import CkipWordSegmenter, CkipPosTagger, CkipNerChunker
import pickle
import logging
import mysql.connector
import time
import spacy
parent_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DIR_PATH = os.path.dirname(os.path.abspath(__file__))
sys.path.append(parent_dir)

from db_func import query_from_db
start = time.time()
# Initialize drivers
ws_driver  = CkipWordSegmenter(level=3)
pos_driver = CkipPosTagger(level=3)
ner_driver = CkipNerChunker(level=3)

os.environ['KMP_DUPLICATE_LIB_OK']= 'True'


FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'title_processor.log'), filemode='a', format=FORMAT)





with open(os.path.join(parent_dir, 'configs', 'loc2server.config'), 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'],
    database = configs['database'])

def insert_title_level_info(title_words: list, title_pos:list , news_id, ws_approach, dep_sentence = ''):
    assert len(title_words) == len(title_pos)
    if ws_approach == 'ckip-transformer':
        for word_index, (word, word_pos) in enumerate(zip(title_words, title_pos)):
            word_cursor = mydb.cursor()
            try:
                word_cursor.execute("INSERT INTO news_db.news_title_words (news_id, word_index, word, word_pos, ws_approach) VALUES (%s, %s, %s, %s, %s)", (news_id, word_index, word, word_pos, ws_approach))
                #print("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, ws_approach) VALUES ({}, {}, {}, {}, {})".format(news_sent_id, word_index, word, word_pos, ws_approach))

            except Exception as e:
                logging.error('Insert word pos error: {}\n News Sent ID: {}'.format(e, news_sent_id))
                print('Insert word pos error: {}\nNews Sent ID: {}\nWs_approach: {}'.format(e, news_sent_id, ws_approach))
                mydb.rollback()
            else:
                #print("else")
                mydb.commit()
            word_cursor.close()
    else:
        logging.error("Error ws_approach")
        sys.exit(0)

def insert_ner_info(entity_title_list, news_id, ner_approach):
    for ent_id, (ent_text, ent_type, ent_index) in enumerate(sorted(entity_title_list, key = lambda x:x[2][0])):
        ner_cursor = mydb.cursor()
        try:
            ner_cursor.execute("INSERT INTO news_db.news_title_ners (news_id, start_index, end_index, ent_type, ent_text, ner_approach) VALUES (%s, %s, %s, %s, %s, %s)", (news_id, ent_index[0], ent_index[1], ent_type, ent_text, ner_approach))
            #print("INSERT INTO news_db.news_ners (news_sent_id, start_index, end_index, ent_type, ent_text, ner_approach) VALUES ({}, {}, {}, {}, {}, {})".format(news_sent_id, ent_index[0], ent_index[1], ent_type, ent_text, ner_approach))
        
        except Exception as e:
            logging.error('Insert NER error: {}\n News ID: {}'.format(e, news_id))
            print('Insert NER error: {}\n News ID: {}'.format(e, news_id))
            mydb.rollback()
        else:
            mydb.commit()
        ner_cursor.close()
        
def insert_process_flag(news_id, process_name):
    process_cursor = mydb.cursor()
    try:
        process_cursor.execute("INSERT INTO news_db.news_processes (news_id, process_name) VALUES (%s, %s)", (news_id, process_name))
        #print("INSERT INTO news_db.nlp_processes (news_sent_id, process_name) VALUES ({}, {})".format(news_sent_id, process_name))
    except Exception as e:
        logging.error('Process Insert Error: {}\nNews ID: {}'.format(e, news_sent_id))
        #print('Process Insert Error: {}\nNews sent ID: {}'.format(e, news_sent_id))
        mydb.rollback()
    else:
        mydb.commit()
    process_cursor.close()

def title_level_analysis(raw_df):
    for index, (news_id, title) in raw_df.iterrows():
        insert_process_flag(news_id, 'title-words')
        process_start = time.time()
        try:
            word_title_list  = ws_driver([title], use_delim=False)
            entity_title_list = ner_driver([title], use_delim=False)
            pos_title_list = pos_driver(word_title_list, use_delim=False)
            print("Ckip process time {} seconds".format(time.time() - process_start))
      
        except Exception as e:
            logging.error('NLP process Error: {}\n News ID: {}'.format(e, news_id))
            print('NLP process Error: {}\n News ID: {}'.format(e, news_id))
        print("Process record in {} seconds".format(time.time() - process_start))
        insert_start = time.time()
        insert_title_level_info(word_title_list[0],  pos_title_list[0], news_id, 'ckip-transformer')
        insert_ner_info(entity_title_list[0], news_id, 'ckip-transformer')
        print("Insert record in {} seconds".format(time.time() - insert_start))
     


raw_df = query_from_db("SELECT news_id, news_title FROM news_db.financial_title_view LIMIT 100;")
print("Finish load the data in {} seconds".format(time.time() - start))
title_level_analysis(raw_df)
mydb.close()
logging.error('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))
print('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))
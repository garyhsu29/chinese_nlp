from ckiptagger import WS, POS, NER
from script_helper import query_from_db
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

with open('server_config_local.pkl', 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'])

def insert_sentlevel_info(word_sentence, pos_sentence, content_sent_id):
    assert len(word_sentence) == len(pos_sentence)
    for word_id, (word, pos) in enumerate(zip(word_sentence, pos_sentence)):
        word_cursor = mydb.cursor()
        try:
            word_cursor.execute("INSERT INTO garysql.sent_words (content_sent_id, word_id, word, word_pos) VALUES (%s, %s, %s, %s)", (content_sent_id, word_id, word, pos))
        except Exception as e:
            logging.error('Insert word pos error: {}\nContent Sent ID: {}'.format(e, content_sent_id))
            mydb.rollback()
        else:
            mydb.commit()
        word_cursor.close()
        
def insert_ner_info(entity_sent_list, content_sent_id):
    for ent_id, (start_index, end_index, ent_type, ent_text) in enumerate(sorted(entity_sent_list, key = lambda x:x[0])):
        ner_cursor = mydb.cursor()
        try:
            ner_cursor.execute("INSERT INTO garysql.sent_ners (content_sent_id, start_index, end_index, ent_type, ent_text) VALUES (%s, %s, %s, %s, %s)", (content_sent_id, start_index, end_index, ent_type, ent_text))
        except Exception as e:
            logging.error('Insert NER error: {}\nContent Sent ID: {}'.format(e, content_sent_id))
            mydb.rollback()
        else:
            mydb.commit()
        ner_cursor.close()
        
def insert_process_flag(content_id, process_name):
    process_cursor = mydb.cursor()
    try:
        process_cursor.execute("INSERT INTO garysql.content_processes (content_id, process_name) VALUES (%s, %s)", (content_id, process_name))
    except Exception as e:
        logging.error('Process Insert Error: {}\nContent ID: {}'.format(e, content_id))
        mydb.rollback()
    else:
        mydb.commit()
    process_cursor.close()

def sent_level_analysis(raw_df):
    for index, (content_sent_ids_str, content_id, sents_str) in raw_df.iterrows():
        insert_process_flag(content_id, 'sent-analysis')
        try:
            sents_lst = ast.literal_eval(sents_str)
            content_sent_ids_lst = ast.literal_eval(content_sent_ids_str)
            assert len(sents_lst) == len(content_sent_ids_lst)
            word_sentence_list = ws(sents_lst)
            pos_sentence_list = pos(word_sentence_list)
            entity_sentence_list = ner(word_sentence_list, pos_sentence_list)
        except Exception as e:
            logging.error('NLP process Error: {}\n Content ID: {}'.format(e, content_id))
        
        for i, sentence in enumerate(sents_lst):
            insert_sentlevel_info(word_sentence_list[i],  pos_sentence_list[i], content_sent_ids_lst[i])
            insert_ner_info(entity_sentence_list[i], content_sent_ids_lst[i])
     

raw_df = query_from_db('SELECT JSON_ARRAYAGG(gcs.content_sent_id) AS content_sent_ids, gcs.content_id, JSON_ARRAYAGG(gcs.sent) AS sents FROM garysql.content_sents gcs WHERE gcs.content_id not in (SELECT gcp.content_id from garysql.content_processes gcp WHERE gcp.process_name = \'sent-analysis\') GROUP BY gcs.content_id LIMIT 5;')
sent_level_analysis(raw_df)
mydb.close()
logging.error('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))

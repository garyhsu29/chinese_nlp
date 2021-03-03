import os, sys
from ckip_transformers.nlp import CkipWordSegmenter, CkipPosTagger, CkipNerChunker
import pickle
import logging
import mysql.connector
import time
import spacy
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
parent_dir = os.path.dirname(DIR_PATH)
sys.path.append(parent_dir)

from db_func import query_from_db, bulk_insert_to_db

FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'sent_processor_bulk.log'), filemode='a', format=FORMAT)
logger = logging.getLogger('sent_process_logger')
load_start = time.time()
# Initialize drivers
ws_driver  = CkipWordSegmenter(level=3, device=0)
pos_driver = CkipPosTagger(level=3, device=0)
ner_driver = CkipNerChunker(level=3, device=0)
print("Load three models in {} seconds".format(time.time() - load_start))
logger.info("Load three models in {} seconds".format(time.time() - load_start))

#nlp = spacy.load("zh_core_web_trf")
os.environ['KMP_DUPLICATE_LIB_OK']= 'True'

def sent_generator(id_lst, sent_lst, batch_size):
    current_idx = 0
    max_len = len(sent_lst)
    while True:
        yield  id_lst[current_idx:current_idx + batch_size], sent_lst[current_idx:current_idx + batch_size]
        current_idx += batch_size
        if current_idx >= max_len:
          break

query_start = time.time()
raw_df = query_from_db("SELECT * FROM news_db.financial_sent_view LIMIT 4096;")
print("Load data from db in {} seconds".format(time.time() - query_start))
logger.info("Load data from db in {} seconds".format(time.time() - query_start))

process_start = time.time()
def process_sents(raw_df):
    news_words_res = []
    news_ners_res = []
    for sent_ids, sents in sent_generator(raw_df['news_sent_id'].tolist(), raw_df['sent'].tolist(), 64):
        word_sentence_list  = ws_driver(sents, batch_size = 64, use_delim=False)
        entity_sentence_list = ner_driver(sents, batch_size = 64, use_delim=False)
        pos_sentence_list = pos_driver(word_sentence_list, batch_size = 64
                                       , use_delim=False)
        for sent_id, word_res, pos_res in zip(sent_ids, word_sentence_list, pos_sentence_list):
            for index, (word, word_pos) in enumerate(zip(word_res, pos_res)):
                news_words_res.append((sent_id, index, word, word_pos, 'ckip-transformer'))
        for sent_id, entities in zip(sent_ids, entity_sentence_list):
            for ent_text, ent_type, ent_index in sorted(entities, key = lambda x:x[2][0]):
                news_ners_res.append((sent_id, ent_index[0], ent_index[1], ent_type, ent_text, 'ckip-transformer'))
    return news_words_res, news_ners_res

news_words_res, news_ners_res = process_sents(raw_df)
news_sent_id_res = []
for sent_id in raw_df['news_sent_id'].unique():
    news_sent_id_res.append((int(sent_id), 'sent-analysis'))

print("Process data in {} seconds".format(time.time() - process_start))
logger.info("Process data in {} seconds".format(time.time() - process_start))

news_process_sql = "INSERT INTO news_db.sent_processes (news_sent_id, process_name) VALUES (%s, %s)"
news_word_sql = "INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, ws_approach) VALUES (%s, %s, %s, %s, %s)"
news_ners_sql = "INSERT INTO news_db.news_ners (news_sent_id, start_index, end_index, ent_type, ent_text, ner_approach) VALUES (%s, %s, %s, %s, %s, %s)"


insert_process_start = time.time()
bulk_insert_to_db(news_process_sql, news_sent_id_res, 1024, logger)
print("Insert ner in {} seconds".format(time.time() - insert_process_start))
logger.info("Insert processed_data in {} seconds".format(time.time() - insert_process_start))

insert_word_start = time.time()
bulk_insert_to_db(news_word_sql, news_words_res, 1024, logger)
print("Insert word in {} seconds".format(time.time() - insert_word_start))
logger.info("Insert word in {} seconds".format(time.time() - insert_word_start))

insert_ner_start = time.time()
bulk_insert_to_db(news_ners_sql, news_ners_res, 1024, logger)
print("Insert ner in {} seconds".format(time.time() - insert_ner_start))
logger.info("Insert ner in {} seconds".format(time.time() - insert_ner_start))


print("Finish all process in {} seconds".format(time.time() - load_start))
logger.info("Finish all process in {} seconds".format(time.time() - load_start))
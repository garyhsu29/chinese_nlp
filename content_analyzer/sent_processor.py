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


nlp = spacy.load("zh_core_web_trf")
os.environ['KMP_DUPLICATE_LIB_OK']= 'True'


FORMAT = '%(asctime)s %(levelname)s: %(message)s'
logging.basicConfig(level=logging.INFO, filename=os.path.join(DIR_PATH, 'logs', 'sent_processor.log'), filemode='a', format=FORMAT)





with open(os.path.join(parent_dir, 'configs', 'server2server.config'), 'rb') as f:
    configs = pickle.load(f)

mydb = mysql.connector.connect(
    host = configs['host'],
    user = configs['user'],
    passwd = configs['passwd'],
    database = configs['database'])

def insert_sentlevel_info(word_sentence: list, pos_sentence:list , news_sent_id, ws_approach, dep_sentence = ''):
    assert len(word_sentence) == len(pos_sentence)
    if ws_approach == 'ckip-transformer':
        for word_index, (word, word_pos) in enumerate(zip(word_sentence, pos_sentence)):
            word_cursor = mydb.cursor()
            try:
                word_cursor.execute("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, ws_approach) VALUES (%s, %s, %s, %s, %s)", (news_sent_id, word_index, word, word_pos, ws_approach))
                #print("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, ws_approach) VALUES ({}, {}, {}, {}, {})".format(news_sent_id, word_index, word, word_pos, ws_approach))

            except Exception as e:
                logging.error('Insert word pos error: {}\n News Sent ID: {}'.format(e, news_sent_id))
                print('Insert word pos error: {}\nNews Sent ID: {}\nWs_approach: {}'.format(e, news_sent_id, ws_approach))
                mydb.rollback()
            else:
                #print("else")
                mydb.commit()
            word_cursor.close()
    elif ws_approach == 'spacy-transformer':
        for word_index, (word, word_pos, word_dep) in enumerate(zip(word_sentence, pos_sentence, dep_sentence)):
            word_cursor = mydb.cursor()
            try:
                word_cursor.execute("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, word_dep, ws_approach) VALUES (%s, %s, %s, %s, %s, %s)", (news_sent_id, word_index, word, word_pos, word_dep, ws_approach))
                #print("INSERT INTO news_db.news_words (news_sent_id, word_index, word, word_pos, word_dep, ws_approach) VALUES ({}, {}, {}, {}, {}, {})".format(news_sent_id, word_index, word, word_pos, word_dep, ws_approach))

            except Exception as e:
                logging.error('Insert word pos error: {}\n News Sent ID: {}'.format(e, news_sent_id))
                print('Insert word pos error: {}\nNews Sent ID: {}\nWs_approach: {}'.format(e, news_sent_id, ws_approach))
                mydb.rollback()
            else:
                mydb.commit()
            word_cursor.close()
    else:
        logging.error("Error ws_approach")
        sys.exit(0)

def insert_ner_info(entity_sent_list, news_sent_id, ner_approach):
    for ent_id, (ent_text, ent_type, ent_index) in enumerate(sorted(entity_sent_list, key = lambda x:x[2][0])):
        ner_cursor = mydb.cursor()
        try:
            ner_cursor.execute("INSERT INTO news_db.news_ners (news_sent_id, start_index, end_index, ent_type, ent_text, ner_approach) VALUES (%s, %s, %s, %s, %s, %s)", (news_sent_id, ent_index[0], ent_index[1], ent_type, ent_text, ner_approach))
            #print("INSERT INTO news_db.news_ners (news_sent_id, start_index, end_index, ent_type, ent_text, ner_approach) VALUES ({}, {}, {}, {}, {}, {})".format(news_sent_id, ent_index[0], ent_index[1], ent_type, ent_text, ner_approach))
        
        except Exception as e:
            logging.error('Insert NER error: {}\n News Sent ID: {}'.format(e, news_sent_id))
            print('Insert NER error: {}\n News Sent ID: {}'.format(e, news_sent_id))
            mydb.rollback()
        else:
            mydb.commit()
        ner_cursor.close()
        
def insert_process_flag(news_sent_id, process_name):
    process_cursor = mydb.cursor()
    try:
        process_cursor.execute("INSERT INTO news_db.sent_processes (news_sent_id, process_name) VALUES (%s, %s)", (news_sent_id, process_name))
        #print("INSERT INTO news_db.nlp_processes (news_sent_id, process_name) VALUES ({}, {})".format(news_sent_id, process_name))
    except Exception as e:
        logging.error('Process Insert Error: {}\nNews sent ID: {}'.format(e, news_sent_id))
        #print('Process Insert Error: {}\nNews sent ID: {}'.format(e, news_sent_id))
        mydb.rollback()
    else:
        mydb.commit()
    process_cursor.close()

def sent_level_analysis(raw_df):
    for index, (content_sent_id, sent) in raw_df.iterrows():
        insert_process_flag(content_sent_id, 'sent-analysis')
        process_start = time.time()
        try:

            word_sentence_list  = ws_driver([sent], use_delim=False)
            entity_sentence_list = ner_driver([sent], use_delim=False)
            pos_sentence_list = pos_driver(word_sentence_list, use_delim=False)
            spacy_doc = nlp(sent)
            word_sent_list_spacy, word_pos_list_spacy, word_dep_list_spacy = zip(*[(token.text, token.tag_, token.dep_) for token in spacy_doc])
            entity_sent_list_spacy = [(ent.text, ent.label_, (ent.start_char, ent.end_char)) for ent in spacy_doc.ents]
        except Exception as e:
            logging.error('NLP process Error: {}\n Content ID: {}'.format(e, content_sent_id))
            print('NLP process Error: {}\n Content ID: {}'.format(e, content_sent_id))
        print("Process record in {} seconds".format(time.time() - process_start))
        insert_start = time.time()
        insert_sentlevel_info(word_sentence_list[0],  pos_sentence_list[0], content_sent_id, 'ckip-transformer')
        insert_sentlevel_info(word_sent_list_spacy, word_pos_list_spacy, content_sent_id, 'spacy-transformer', word_dep_list_spacy)
        insert_ner_info(entity_sentence_list[0], content_sent_id, 'ckip-transformer')
        insert_ner_info(entity_sent_list_spacy, content_sent_id, 'spacy-transformer')
        print("Insert record in {} seconds".format(time.time() - insert_start))
     


raw_df = query_from_db("SELECT * FROM news_db.sent_source_view LIMIT 100;")
print("finish load the data in {} seconds".format(time.time() - start))
sent_level_analysis(raw_df)
mydb.close()
logging.error('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))
print('Finish process {} examples in {} seconds'.format(len(raw_df), time.time() - start))
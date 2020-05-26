from sqlalchemy import create_engine
import pymysql
import pandas as pd
import pickle
import os

DIR_PATH = os.path.dirname(os.path.realpath(__file__))
with open('server_config_local.pkl', 'rb') as f:
    configs = pickle.load(f)
    
def query_from_db(sql):
	#'mysql+pymysql://gary:22213620@127.0.0.1/garysql'
    db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(configs['user'], configs['passwd'], configs['host'], configs['db'])
    db_connection = create_engine(db_connection_str)
    df = pd.read_sql(sql, con=db_connection)
    return df

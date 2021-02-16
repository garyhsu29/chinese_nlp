import pymysql
import pandas as pd
import os
import pickle
 
DIR_PATH = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(DIR_PATH, 'configs', 'server2server.config'), 'rb') as f:
    configs = pickle.load(f)


def query_from_db(sql):
    connection = pymysql.connect(user = configs['user'], password = configs['passwd'], database = configs['database'], host = configs['host'])
    #db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(configs['user'], configs['passwd'], configs['host'], configs['database'])
    #db_connection = create_engine(db_connection_str)
    df = pd.read_sql(sql, con=connection)
    connection.close()
    return df

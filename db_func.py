import pymysql
import pandas as pd
import os
import pickle
 
DIR_PATH = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(DIR_PATH, 'configs', 'loc2server.config'), 'rb') as f:
    configs = pickle.load(f)

def data_loader(values, page_size):
    max_len = len(values)
    current_idx = 0
    while True:
        yield values[current_idx: current_idx+ page_size]
        current_idx += page_size
        if current_idx >= max_len:
            break


def query_from_db(sql):
    connection = pymysql.connect(user = configs['user'], password = configs['passwd'], database = configs['database'], host = configs['host'])
    #db_connection_str = 'mysql+pymysql://{}:{}@{}/{}'.format(configs['user'], configs['passwd'], configs['host'], configs['database'])
    #db_connection = create_engine(db_connection_str)
    df = pd.read_sql(sql, con=connection)
    connection.close()
    return df

def bulk_insert_to_db(sql, values, page_size, logger):
    connection = pymysql.connect(user = configs['user'], password = configs['passwd'], database = configs['database'], host = configs['host'])
    for val in data_loader(values, page_size):
        try:
            cursor = connection.cursor()
            # Execute sql statement
            cursor.executemany(sql, val)
            # Submit to database execution
            connection.commit()
        except Exception as e:
            # Roll back if an error occurs
            connection.rollback()
            logger.error("Inser Error: {}\nSQL:{}".format(e, sql))
            print("Inser Error: {}\nSQL:{}".format(e, sql))
        else:
        	print("Insert {} rows successfully".format(len(val)))
    connection.close()
    return 



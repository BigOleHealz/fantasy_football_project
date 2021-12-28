import pymysql, json
import pandas as pd
import requests
from DBConn import DBConn
from sqlalchemy import create_engine
import numpy as np
# from pandas.io.json import json_normalize

pd.set_option('display.max_rows', 500)

tablename = 'player_scoring_plays'
primary_id = '_id'
important_column = 'ScoringDetails'


def map_types(data):
    vals = {
        str : 'VARCHAR(255)',
        int : 'INT'
    }
    return {k : vals[type(v)] for k, v in data.items()}

def populate_scoring_plays(host, user, password, port, db):
    try:
        handler = DBConn(host, user, password, port, db)
        connection = handler.get_connection()

        response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
        resp = json.loads(response.content.decode())

        idx = 0
        while len(resp[idx][important_column]) < 1:
            idx += 1
        sample = resp[idx][important_column][0]
        df_columns = sample.keys()

        dtypes = {k : type(v) for k, v in sample.items()}
        df = pd.json_normalize(resp)

        schema = map_types(sample)
        handler.make_table(tablename, schema=schema, primary_key=primary_id, auto_increment=True)
        
        data = pd.DataFrame(columns=df_columns)
        for i in range(1, 16):
            print(i)
            response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
            resp = json.loads(response.content.decode())
            df = pd.json_normalize(resp)[[important_column]]
            for i, row in df.iterrows():
                if len (row[important_column]):
                    for rec in row:
                        data = pd.concat([data, pd.DataFrame(rec)])
            
            data = pd.concat([data, df])
        data.drop(labels='ScoringDetails', axis=1, inplace=True)
        data.dropna(how='all', inplace=True)
        
        conn_str = 'mysql+mysqldb://' + user + ':' + password + '@' + host + f':{port}/' \
        + db + '?charset=utf8'
        engine = create_engine(conn_str)
        
        cursor=connection.cursor()
        data.to_sql(tablename, engine, if_exists='append', index=False)
        connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    host = "localhost"
    user = "root"
    password = ""
    port = 3308
    db = "fantasy_db"
    populate_scoring_plays(host, user, password, port, db)
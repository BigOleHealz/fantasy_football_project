import pymysql, json
import pandas as pd
import requests
from DBConn import DBConn
from sqlalchemy import create_engine

tablename = 'game_stats'
primary_id = 'PlayerGameID'

def map_types(data):
    vals = {
        'object' : 'VARCHAR(255)',
        'int64' : 'INT',
        'float64' : 'DOUBLE(6,2)',
        'bool' : 'BINARY'
    }
    return {k : vals[str(v)] for k, v in data.items()}

def populate_game_stats(host, user, password, port, db):
    try:
        handler = DBConn(host, user, password, port, db)
        connection = handler.get_connection()

        response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
        resp = json.loads(response.content.decode())
        df = pd.json_normalize(resp)
        df.drop(labels='ScoringDetails', axis=1, inplace=True)
        schema = map_types(df.dtypes.to_dict())
        handler.make_table(tablename, schema=schema, primary_key=primary_id)
        
        for i in range(1, 16):
            response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
            resp = json.loads(response.content.decode())
            df = pd.json_normalize(resp)
            df.drop(labels='ScoringDetails', axis=1, inplace=True)

            conn_str = 'mysql+mysqldb://' + user + ':' + password + '@' + host + f':{port}/' \
            + db + '?charset=utf8'
            engine = create_engine(conn_str)

            cursor=connection.cursor()
            df.to_sql(tablename, engine, if_exists='append', index=False)
        connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    populate_game_stats()
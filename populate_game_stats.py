import pymysql, json
import pandas as pd
import requests
from DBConn import DBConn
from sqlalchemy import create_engine

tablename = 'game_stats'
primary_id = 'PlayerGameID'

def map_types(data):
    vals = {
        'object' : 'TEXT',
        'int64' : 'INT',
        'float64' : 'DOUBLE(6,2)',
        'bool' : 'BINARY'
    }
    return {k : vals[str(v)] for k, v in data.items()}

def populate_game_stats(db_filename: str):
    try:
        handler = DBConn(db_filename)
        connection = handler.get_connection()

        response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
        resp = json.loads(response.content.decode())
        df = pd.json_normalize(resp)
        df.drop(labels='ScoringDetails', axis=1, inplace=True)
        schema = map_types(df.dtypes.to_dict())
        handler.make_table(tablename, schema=schema, primary_key=primary_id)
        
        for i in range(1, 2):
            response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
            resp = json.loads(response.content.decode())
            df = pd.json_normalize(resp)
            df.drop(labels='ScoringDetails', axis=1, inplace=True)

            df.to_sql(tablename, connection, if_exists='append', index=False)
        connection.commit()
    finally:
        connection.close()

if __name__ == "__main__":
    populate_game_stats()
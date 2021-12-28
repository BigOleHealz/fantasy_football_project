import pymysql, json
import pandas as pd
import requests
from DBConn import DBConn
from sqlalchemy import create_engine

tablename = 'players'
primary_id = 'PlayerID'

def map_types(data):
    vals = {
        'object' : 'VARCHAR(255)',
        'int64' : 'INT',
        'float64' : 'DOUBLE(6,2)',
        'bool' : 'BINARY'
    }
    return {k : vals[str(v)] for k, v in data.items()}

def populate_players(host, user, password, port, db):
    try:
        handler = DBConn(host, user, password, port, db)
        connection = handler.get_connection()

        response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
        resp = json.loads(response.content.decode())
        df_columns = [primary_id, 'ShortName', 'Position']
        df = pd.json_normalize(resp)[df_columns]
        schema = map_types(df.dtypes.to_dict())
        handler.make_table(tablename, schema=schema, primary_key=primary_id)
        
        data = pd.DataFrame(columns=df_columns)
        for i in range(1, 16):
            response = requests.get(f'https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}',
                    headers={'Ocp-Apim-Subscription-Key' : '61d6cd3fc2c44db1a6596211aa25b399'})
            resp = json.loads(response.content.decode())
            df = pd.json_normalize(resp)[df_columns]
            data = pd.concat([data, df])

        data.drop_duplicates(subset=primary_id, inplace=True)
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
    populate_players(host, user, password, port, db)
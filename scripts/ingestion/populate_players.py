import argparse
import logging
import os
import sys
from pathlib import Path
import requests
import json

import pandas as pd
from dotenv import load_dotenv

from ff_db import Players, PlayerScoringPlays, GameStats, Session, engine

tablename = "players"
primary_id = "PlayerID"


def populate_players(tablename, engine, if_exists, index):

    response = requests.get(
        f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}",
        headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
    )
    resp = json.loads(response.content.decode())
    df_columns = [primary_id, "ShortName", "Position"]
    df = pd.json_normalize(resp)[df_columns]

    data = pd.DataFrame(columns=df_columns)
    for i in range(1, 16):
        response = requests.get(
            f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}",
            headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
        )
        resp = json.loads(response.content.decode())
        df = pd.json_normalize(resp)[df_columns]
        data = pd.concat([data, df])

    data.drop_duplicates(subset=primary_id, inplace=True)

    data.to_sql(name=tablename, con=engine, if_exists=if_exists, index=index)


if __name__ == "__main__":
    populate_players(
        tablename=tablename, engine=engine, if_exists="append", index=False
    )

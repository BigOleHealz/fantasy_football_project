import argparse
import logging
import os
import sys
from pathlib import Path
import requests
import json
import numpy as np

import pandas as pd
from dotenv import load_dotenv

from ff_db import Players, PlayerScoringPlays, GameStats, Session, engine

tablename = "player_scoring_plays"
primary_id = "_id"
important_column = "ScoringDetails"


def populate_scoring_plays(tablename, engine, if_exists, index):

    response = requests.get(
        f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}",
        headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
    )
    resp = json.loads(response.content.decode())

    idx = 0
    while len(resp[idx][important_column]) < 1:
        idx += 1
    sample = resp[idx][important_column][0]
    df_columns = sample.keys()

    df = pd.json_normalize(resp)

    data = pd.DataFrame(columns=df_columns)
    for i in range(1, 16):
        print(i)
        response = requests.get(
            f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}",
            headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
        )
        resp = json.loads(response.content.decode())
        df = pd.json_normalize(resp)[[important_column]]
        for i, row in df.iterrows():
            if len(row[important_column]):
                for rec in row:
                    data = pd.concat([data, pd.DataFrame(rec)])

        data = pd.concat([data, df])
    data.drop(labels="ScoringDetails", axis=1, inplace=True)
    data.dropna(how="all", inplace=True)
    data["index"] = np.arange(len(data))

    data.to_sql(name=tablename, con=engine, if_exists=if_exists, index=index)


if __name__ == "__main__":
    populate_scoring_plays(
        tablename=tablename, engine=engine, if_exists="append", index=False
    )

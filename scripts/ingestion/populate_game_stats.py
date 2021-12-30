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

tablename = "game_stats"


def populate_game_stats(tablename, engine, if_exists, index):
    response = requests.get(
        f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{1}",
        headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
    )
    resp = json.loads(response.content.decode())
    df = pd.json_normalize(resp)
    df.drop(labels="ScoringDetails", axis=1, inplace=True)

    for i in range(1, 16):
        response = requests.get(
            f"https://api.sportsdata.io/v3/nfl/stats/json/PlayerGameStatsByWeek/2021/{i}",
            headers={"Ocp-Apim-Subscription-Key": "61d6cd3fc2c44db1a6596211aa25b399"},
        )
        resp = json.loads(response.content.decode())
        df = pd.json_normalize(resp)
        df.drop(labels="ScoringDetails", axis=1, inplace=True)

        df.to_sql(
            name=tablename, con=engine, if_exists=if_exists, index=index, multi=True
        )


if __name__ == "__main__":
    populate_game_stats(
        tablename=tablename, engine=engine, if_exists="append", index=False
    )

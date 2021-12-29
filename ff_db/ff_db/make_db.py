import argparse
import os
import sys

from db_connection import engine
from ff_db import Players, PlayerScoringPlays, GameStats, Base

parser = argparse.ArgumentParser(description="Create Database")
parser.add_argument("--create", help="Create the database", type=bool, required=False)
args = parser.parse_args()
if args.create == True:
    Base.metadata.create_all(engine)
else:
    pass

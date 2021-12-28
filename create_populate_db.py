import requests
import pymysql
import json
import pandas as pd
from DBConn import DBConn
from populate_game_stats import populate_game_stats
from populate_players import populate_players
from populate_scoring_plays import populate_scoring_plays

host = "localhost"
user = "root"
password = ""
port = 3308
db = "fantasy_db"

def create_db():
    handler = DBConn(host, user, password, port, db)
    handler.create_db()

def create_populate_game_stats_table():
    populate_game_stats(host, user, password, port, db)

def populate_players_table():
    populate_players(host, user, password, port, db)

def populate_scoring_plays_table():
    populate_players(host, user, password, port, db)



if __name__ == "__main__":
    create_db()
    create_populate_game_stats_table()
    populate_players_table()
    populate_scoring_plays_table()
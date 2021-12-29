import os
import sys

from sqlalchemy import create_engine

from .db import Players, PlayerScoringPlays, GameStats, Base
from .db_connection import DB_URL, Session, engine

DB_URL = DB_URL

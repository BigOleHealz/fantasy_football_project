import os
import sys

from dotenv import load_dotenv
from sqlalchemy import create_engine
from sqlalchemy.orm import relationship, scoped_session, sessionmaker

load_dotenv()
DB_URL = os.getenv("DB_URL")
print("DB_URL:", DB_URL)
engine = create_engine(DB_URL, echo=False)
Session = scoped_session(sessionmaker(bind=engine))

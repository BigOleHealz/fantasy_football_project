# fantasy_football_project

### Install and activate python environment
```
make install
source venv/bin/activate
```

### create .env file in top level directory with contents
```
DB_URL = sqlite:////absolute_path_to_db.db
```

### create database
```
python ./ff_db/ff_db/make_db.py --create True
```

### populate tables
```
python ./scripts/ingestion/populate_scoring_plays.py
python ./scripts/ingestion/populate_game_stats.py
python ./scripts/ingestion/populate_players.py
```

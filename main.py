import logging
import logging.handlers
import os
import supabase

import requests
from scripts import station_data_cleanse
from scripts import entrances_query
from scripts import entrances_data_cleanse
from scripts import load_sql
from scripts import load_supabase



logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
logger_file_handler = logging.handlers.RotatingFileHandler(
    "status.log",
    maxBytes=1024 * 1024,
    backupCount=1,
    encoding="utf8",
)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger_file_handler.setFormatter(formatter)
logger.addHandler(logger_file_handler)

try:
    #SOME_SECRET = os.environ["SOME_SECRET"]
    url: str = os.environ["SUPABASE_URL"]
    key: str = os.environ["SUPABASE_KEY"]
except KeyError:
    #SOME_SECRET = "Token not available!"
    url = "URL not available!"
    key = "key not available"
    #logger.info("Token not available!")
    #raise



if __name__ == "__main__":
    
    #logger.info(f"Token value: {SOME_SECRET}")
    logger.info("Running station data cleansing process")
    station_data_cleanse.run()    
    logger.info("Running entrance data query process")
    entrances_query.run()
    logger.info("Running entrance data cleansing process")
    entrances_data_cleanse.run()
    logger.info("Loading CSV data into SQLite")
    load_sql.run()
    logger.info("Loading CSV data into Supabase")
    load_supabase.run(url,key)


    ##r = requests.get('https://weather.talkpython.fm/api/weather/?city=Berlin&country=DE')
    ##if r.status_code == 200:
    #    data = r.json()
     #   temperature = data["forecast"]["temp"]
     #   logger.info(f'Weather in Berlin: {temperature}')
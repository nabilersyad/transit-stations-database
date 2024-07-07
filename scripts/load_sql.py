import pandas as pd
import sqlite3
import os

logger = logging.getLogger(__name__)

def run():
    logger.info("Load csv data into sqlite")

    # Define the directory of cleansed data to transfer to sql
    cleansed_data_directory = 'data_cleansed'
    cleansed_kl_file = 'klang_valley_stations_cleansed.csv'
    cleansed_montreal_file = 'montreal_stations_cleansed.csv'
    cleansed_singapore_file = 'singapore_stations_cleansed.csv'
    cleansed_combined_file = 'combined_stations_cleansed.csv'

    # read cleaned dataframes
    kl_data = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_kl_file))
    montreal_data = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_montreal_file))
    singapore_data = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_singapore_file))


    # Combine all the dataframes
    combined_df = pd.concat([kl_data, montreal_data, singapore_data], axis=0, ignore_index=True)

    combined_df.index.name = 'station_id'
    combined_df.to_csv(os.path.join(cleansed_data_directory, cleansed_combined_file), index=True)


    # Create a connection to the SQLite database
    # Doesn't matter if the database does not yet exist
    conn = sqlite3.connect('transit_database.db')  

    # Add the station data from the combined dataframe to the SQLite table
    combined_df.to_sql('stations', conn, if_exists='replace')


    cleansed_entrances = 'klang_valley_entrances_cleansed.csv'
    cleansed_station_entrances= 'klang_valley_stations_entrances_relation_cleansed.csv'

    # read cleaned entrances dataframes
    entrances_data = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_entrances))
    station_entrances_data = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_station_entrances))

    # Add the entrances data from the combined dataframe to the SQLite table
    entrances_data.to_sql('entrances', conn, if_exists='replace', index=False)
    station_entrances_data.to_sql('station_entrances', conn, if_exists='replace', index=False)


    # Commit the changes and close the connection
    conn.commit()
import pandas as pd
import os
import logging


logger = logging.getLogger(__name__)


def run():
    logger.info("Cleanse entrance csv data")

    data_directory = 'data'
    entrances_file = 'klang_valley_entrances.csv'
    station_entrances_file = 'klang_valley_stations_entrances_relation.csv'

    entrances_data = pd.read_csv(os.path.join(data_directory, entrances_file))
    station_entrances_data  = pd.read_csv(os.path.join(data_directory, station_entrances_file))

    # split the Station ID column into multiple rows
    station_entrances_data = (station_entrances_data.set_index(['Relationship ID', 'Entrance ID', 'Station Name'])
        .apply(lambda x: x.str.split(';').explode())
        .reset_index())

    # reset the Relationship ID
    station_entrances_data['Relationship ID'] = range(len(station_entrances_data))


    # Rename columns
    entrances_data = entrances_data.rename(columns={
        'Station ID': 'station_code',
    })
    # Rename columns
    station_entrances_data = station_entrances_data.rename(columns={
        'Station ID': 'station_code',
    })

    # Define the directory where you want to save the cleaned data
    cleansed_data_directory = 'data_cleansed'
    cleansed_entrances_file= 'klang_valley_entrances_cleansed.csv'
    cleansed_station_entrances_file = 'klang_valley_stations_entrances_relation_cleansed.csv'

    # Save the cleaned dataframes
    entrances_data.to_csv(os.path.join(cleansed_data_directory, cleansed_entrances_file), index=False)
    station_entrances_data.to_csv(os.path.join(cleansed_data_directory, cleansed_station_entrances_file), index=False)




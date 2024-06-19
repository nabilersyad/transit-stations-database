import pandas as pd
import os
import logging



logger = logging.getLogger(__name__)


def run():
    data_directory = 'data'
    kl_file = 'klang_valley_stations.csv'
    montreal_file = 'montreal_metro.csv'
    singapore_file = 'mrtsg.csv'

    logger.info("Running station data cleansing process")
    kl_data = pd.read_csv(os.path.join(data_directory, kl_file))
    montreal_data = pd.read_csv(os.path.join(data_directory, montreal_file))
    singapore_data = pd.read_csv(os.path.join(data_directory, singapore_file))

    # We would now normalize/standardize the column names of the dataframe to ensure consistency
    kl_data.columns = kl_data.columns.str.lower().str.replace(' ', '_')
    montreal_data.columns = montreal_data.columns.str.lower().str.replace(' ', '_')
    singapore_data.columns = singapore_data.columns.str.lower().str.replace(' ', '_')



    #ensure only first letter of a column is capitalized
    kl_data['name'] = kl_data['name'].str.title()
    montreal_data['name'] = montreal_data['name'].str.title()
    singapore_data['line_colour'] = singapore_data['line_colour'].str.title()




    #drop irrelevant columns
    montreal_data=montreal_data.drop(columns=['index'])
    singapore_data=singapore_data.drop(columns=['objectid','x','y'])


    # Rename columns
    kl_data = kl_data.rename(columns={
        'city': 'region',
        'stop_id':'station_code'
    })
    montreal_data = montreal_data.rename(columns={
        '_stop_id': 'station_code',
        'city':'region'
    })
    # Rename columns
    singapore_data = singapore_data.rename(columns={
        'stn_no': 'station_code',
        'route_code':'route_id',
        'city':'region'
    })


    # Rename rows

    kl_data['name'] = kl_data['name'].replace({
        "Kl Sentral": "KL Sentral",
        "Ukm": "UKM",
        "Pwtc": "PWTC",
        "Taman Perindustrian Puchong (Tpp)":"Taman Perindustrian Puchong (TPP)",
        "Klcc":"KLCC",
        "Ss15":"SS15",
        "Ss18": "SS18", 
        "Usj7" : "USJ7",
        "Usj21" : "USJ21",
        "Klia2" : "KLIA2",
        "Ttdi" : "TTDI",
        "Sunu Monash" : "SunU Monash",
        "South Quay-Usj1" : "South Quay-USJ1",
        "Persiaran Klcc" : "Persiaran KLCC",
        "Upm" : "UPM",
        "Bu11" : "BU11",
        "Ss7" : "SS7",
        "Uitm" : "UiTM"
                })

    # Define the directory where you want to save the cleaned data
    cleansed_data_directory = 'data_cleansed'
    cleansed_kl_file = 'klang_valley_stations_cleansed.csv'
    cleansed_montreal_file = 'montreal_stations_cleansed.csv'
    cleansed_singapore_file = 'singapore_stations_cleansed.csv'

    # Save the cleaned dataframes
    kl_data.to_csv(os.path.join(cleansed_data_directory, cleansed_kl_file), index=False)
    montreal_data.to_csv(os.path.join(cleansed_data_directory, cleansed_montreal_file), index=False)
    singapore_data.to_csv(os.path.join(cleansed_data_directory, cleansed_singapore_file), index=False)

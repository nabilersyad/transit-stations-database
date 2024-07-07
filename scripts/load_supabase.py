import pandas as pd
import os
from supabase import create_client, Client
import logging

logger = logging.getLogger(__name__)

def run(url,key):
    logger.info("load data to Supabase")
    #declaring the supabase client we will working with
    #url: str = os.environ.get("SUPABASE_URL")
    #key: str = os.environ.get("SUPABASE_KEY")
    supabase: Client = create_client(url, key)


    ##Load Stations Table

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
    stations_data_local = pd.concat([kl_data, montreal_data, singapore_data], axis=0, ignore_index=True)


    #ensure NAs are in a posgres readable format
    stations_data_local = stations_data_local.where(pd.notna(stations_data_local), None)

    stations_data_local.index.name = 'station_id'
    stations_data_local.to_csv(os.path.join(cleansed_data_directory, cleansed_combined_file), index=True)

    response = supabase.table('stations').select("*").execute()
    data,_ = response
    stations_data_supa = pd.DataFrame(data[1])
    stations_data_supa.set_index('station_id',inplace=True)

    # Perform an outer join on the dataframes
    merged_stations_data = pd.merge(stations_data_local, stations_data_supa, how='outer',left_index=True,right_index=True,indicator=True)

    # Case 1 and 2: rows exist in both dataframes
    both_stations_data = merged_stations_data[merged_stations_data['_merge'] == 'both']
    both_stations_data = both_stations_data.drop(columns=['_merge'])

    merged_stations_data['_merge'].value_counts()

    #Case 1 
    #The supabase table does not need to be updated
    identical_stations_rows_mask = [stations_data_local.loc[i, stations_data_local.columns].equals(stations_data_supa.loc[i, stations_data_supa.columns])
                            for i in both_stations_data.index]
    identical_stations_rows = both_stations_data[identical_stations_rows_mask]
    identical_stations_rows = stations_data_local.loc[identical_stations_rows.index]

    #Case 2
    #Updating rows in Supabase with values from CSV
    different_stations_rows_mask= [not b for b in identical_stations_rows_mask]
    different_stations_rows = both_stations_data[different_stations_rows_mask]

    # Replace all NaN values in the DataFrame with None
    different_stations_rows = different_stations_rows.where(pd.notna(different_stations_rows), None)
    different_stations_rows = stations_data_local.loc[different_stations_rows.index]

    #the following is to convert index in pandas to be a column, usable for posgres
    different_stations_rows = different_stations_rows.reset_index(drop=False)

    # For the different rows, you want to update data_supabase with data from data_local
    for index, row in different_stations_rows.iterrows():
        # Use Supabase update method
        # Note: replace 'id' and 'your_table' with your actual id column name and table name
        data, error = supabase.table('stations').update(row.to_dict()).eq('station_id', row['station_id']).execute()


    # Case 3
    # Insert rows in data_local but not in data_supabase into Supabase

    only_local_stations = merged_stations_data[merged_stations_data['_merge'] == 'left_only']
    only_local_stations = only_local_stations.drop(columns=['_merge'])

    only_local_stations = stations_data_local.loc[only_local_stations.index]
    only_local_stations = only_local_stations.where(pd.notna(only_local_stations), None)
    only_local_stations = only_local_stations.reset_index(drop=False)

    # For these rows, you want to insert into data_supabase
    # Check if DataFrame is empty
    if not only_local_stations.empty:
        data, error = supabase.table('stations').insert(only_local_stations.to_dict('records')).execute()
    else:
        print("No new rows to be inserted")


    # Case 4
    # Delete rows not in data_local but in data_supabase
    only_supabase_stations = merged_stations_data[merged_stations_data['_merge'] == 'right_only']
    only_supabase_stations = only_supabase_stations.drop(columns=['_merge'])

    only_supabase_stations = stations_data_local.loc[only_supabase_stations.index]
    only_supabase_stations = only_supabase_stations.where(pd.notna(only_supabase_stations), None)
    only_supabase_stations = only_supabase_stations.reset_index(drop=False)

    # For these rows, you want to delete from data_supabase
    for index, row in only_supabase_stations.iterrows():
        # Use Supabase delete method
        data, error = supabase.table('stations').delete().eq('station_id', row['station_id']).execute()

    ## Load Entrances Table
    # Retrieve entrances table in local csv file
    cleansed_entrances = 'klang_valley_entrances_cleansed.csv'

    # read cleaned entrances dataframes
    entrances_data_local = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_entrances))

    #ensure NAs are in a posgres readable format
    entrances_data_local = entrances_data_local.where(pd.notna(entrances_data_local), None)

    #ensure index to be entrance_id because it is easier to work with
    entrances_data_local.set_index('entrance_id',inplace=True)
    entrances_data_local = entrances_data_local.sort_values(by='entrance_id')

    # Retrieve entrances table from supabase

    response = supabase.table('entrances').select("*").execute()
    data,_ = response
    entrances_data_supa = pd.DataFrame(data[1])

    #ensure index to be entrance_id because it is easier to work with
    entrances_data_supa.set_index('entrance_id',inplace=True)
    entrances_data_supa = entrances_data_supa.sort_values(by='entrance_id')

    # Perform an outer join on the dataframes
    merged_data = pd.merge(entrances_data_local,entrances_data_supa,how='outer',left_index=True,right_index=True,indicator=True)

    # Case 1 and 2: rows exist in both dataframes
    both_data = merged_data[merged_data['_merge'] == 'both']
    both_data = both_data.drop(columns=['_merge'])

    merged_data['_merge'].value_counts()
    
    #Case 1 
    #The supabase table does not need to be updated
    identical_rows_mask = [entrances_data_local.loc[i, entrances_data_local.columns].equals(entrances_data_supa.loc[i, entrances_data_supa.columns])
                        for i in both_data.index]
    identical_rows = both_data[identical_rows_mask]
    identical_rows = entrances_data_local.loc[identical_rows.index]

    #Case 2
    #Updating rows in Supabase with values from CSV
    different_rows_mask= [not b for b in identical_rows_mask]
    different_rows = both_data[different_rows_mask]

    # Replace all NaN values in the DataFrame with None
    different_rows = different_rows.where(pd.notna(different_rows), None)
    different_rows = entrances_data_local.loc[different_rows.index]

    #the following is to convert index in pandas to be a column, usable for posgres
    different_rows = different_rows.reset_index(drop=False)

    # For the different rows, you want to update data_supabase with data from data_local
    for index, row in different_rows.iterrows():
        # Use Supabase update method
        # Note: replace 'id' and 'your_table' with your actual id column name and table name
        data, error = supabase.table('entrances').update(row.to_dict()).eq('entrance_id', row['entrance_id']).execute()

    # Case 3
    # Insert rows in data_local but not in data_supabase into Supabase

    only_local = merged_data[merged_data['_merge'] == 'left_only']
    only_local = only_local.drop(columns=['_merge'])

    only_local = entrances_data_local.loc[only_local.index]
    only_local = only_local.where(pd.notna(only_local), None)
    only_local = only_local.reset_index(drop=False)

    # For these rows, you want to insert into data_supabase
    if not only_local.empty:
        data, error = supabase.table('station_entrances').insert(only_local.to_dict('records')).execute()
    else:
        print("No new rows to be inserted")

    # Case 4
    # Delete rows not in data_local but in data_supabase
    only_supabase = merged_data[merged_data['_merge'] == 'right_only']
    only_supabase = only_supabase.drop(columns=['_merge'])

    only_supabase = entrances_data_supa.loc[only_supabase.index]
    only_supabase = only_supabase.where(pd.notna(only_supabase), None)
    only_supabase = only_supabase.reset_index(drop=False)

    # For these rows, you want to delete from data_supabase
    for index, row in only_supabase.iterrows():
        # Use Supabase delete method
        data, error = supabase.table('entrances').delete().eq('entrance_id', row['entrance_id']).execute()

   ## Load Station Entrances Table
    # Retrieve entrances table in local csv file

    # Retrieve station entrances table in local csv file
    cleansed_station_entrances= 'klang_valley_stations_entrances_relation_cleansed.csv'

    # read cleaned statuion entrances dataframes
    station_entrances_data_local = pd.read_csv(os.path.join(cleansed_data_directory, cleansed_station_entrances))

    #ensure NAs are in a posgres readable format
    station_entrances_data_local = station_entrances_data_local.where(pd.notna(station_entrances_data_local), None)

    #ensure index to be entrance_id because it is easier to work with
    station_entrances_data_local.set_index('relationship_id',inplace=True)
    station_entrances_data_local = station_entrances_data_local.sort_values(by='entrance_id')

    # Retrieve entrances table from supabase

    response = supabase.table('station_entrances').select("*").execute()
    data,_ = response
    station_entrances_data_supa = pd.DataFrame(data[1])

    #ensure index to be entrance_id because it is easier to work with
    station_entrances_data_supa.set_index('relationship_id',inplace=True)
    station_entrances_data_supa = station_entrances_data_supa.sort_values(by='relationship_id')


    # Perform an outer join on the dataframes
    merged_station_entrances_data = pd.merge(station_entrances_data_local,station_entrances_data_supa,how='outer',left_index=True,right_index=True,indicator=True)

    # Case 1 and 2: rows exist in both dataframes
    both_station_entrances_data = merged_station_entrances_data[merged_station_entrances_data['_merge'] == 'both']
    both_station_entrances_data = both_station_entrances_data.drop(columns=['_merge'])

    merged_station_entrances_data['_merge'].value_counts()

    #Case 1 
    #The supabase table does not need to be updated
    identical_station_entrances_rows_mask = [station_entrances_data_local.loc[i, station_entrances_data_local.columns].equals(station_entrances_data_supa.loc[i, station_entrances_data_supa.columns])
                            for i in both_station_entrances_data.index]
    identical_station_entrances_rows = both_station_entrances_data[identical_station_entrances_rows_mask]
    identical_station_entrances_rows = station_entrances_data_local.loc[identical_station_entrances_rows.index]


    #Case 2
    #Updating rows in Supabase with values from CSV
    different_station_entrances_rows_mask= [not b for b in identical_station_entrances_rows_mask]
    different_station_entrances_rows = both_station_entrances_data[different_station_entrances_rows_mask]

    # Replace all NaN values in the DataFrame with None
    different_station_entrances_rows = different_station_entrances_rows.where(pd.notna(different_station_entrances_rows), None)
    different_station_entrances_rows = station_entrances_data_local.loc[different_station_entrances_rows.index]

    different_station_entrances_rows

    # Case 3
    # Insert rows in data_local but not in data_supabase into Supabase

    only_local_station_entrances = merged_station_entrances_data[merged_station_entrances_data['_merge'] == 'left_only']
    only_local_station_entrances = only_local_station_entrances.drop(columns=['_merge'])

    only_local_station_entrances = station_entrances_data_local.loc[only_local_station_entrances.index]
    only_local_station_entrances = only_local_station_entrances.where(pd.notna(only_local_station_entrances), None)
    only_local_station_entrances = only_local_station_entrances.reset_index(drop=False)

    # For these rows, you want to insert into data_supabase
    if not only_local_station_entrances.empty:
        data, error = supabase.table('station_entrances').insert(only_local_station_entrances.to_dict('records')).execute()
    else:
        print("No new rows to be inserted")

    # Case 4
    # Delete rows not in data_local but in data_supabase    
    only_supabase_station_entrances = merged_station_entrances_data[merged_station_entrances_data['_merge'] == 'right_only']
    only_supabase_station_entrances = only_supabase_station_entrances.drop(columns=['_merge'])

    only_supabase_station_entrances = station_entrances_data_local.loc[only_supabase_station_entrances.index]
    only_supabase_station_entrances = only_supabase_station_entrances.where(pd.notna(only_supabase_station_entrances), None)
    only_supabase_station_entrances = only_supabase_station_entrances.reset_index(drop=False)

    # For these rows, you want to delete from data_supabase
    for index, row in only_supabase_station_entrances.iterrows():
        # Use Supabase delete method
        data, error = supabase.table('station_entrances').delete().eq('relationship_id', row['relationship_id']).execute()
import geopandas as gpd
import overpy
import pandas as pd
import os
from datetime import date
import logging


logger = logging.getLogger(__name__)

def run():
    logger.info("Querying entrance and station data in OSM")

    # initialize Overpass API
    api = overpy.Overpass()

    # DataFrames for entrances and station-entrance relationships
    entrances = pd.DataFrame(columns=['Entrance ID', 'Longitude','Latitude','Entrance Destination'])
    station_entrances = pd.DataFrame(columns=['Relationship ID', 'Entrance ID', 'Station Code','Station Name'])

    # Query all subway entrances in Malaysia
    result_entrances = api.query("""
    [out:json][timeout:25];
    area["name"="Malaysia"]["boundary"="administrative"]->.searchArea;
    node["railway"~"subway_entrance|train_station_entrance"](area.searchArea);
    out body;
    """)
    print(f'Entrance query completed {date.today()}')


    # Store the subway_entrance object IDs and coordinates in df_entrances
    for node in result_entrances.nodes:
        entrances = pd.concat(
            [entrances, 
            pd.DataFrame([{'Entrance ID': node.id, 
                            'Entrance Name':node.tags.get('ref'),
                            'Entrance Destination':node.tags.get('destination'),
                            'Longitude':node.lon,
                            'Latitude':node.lat}])], 
            ignore_index=True
        )

    #  Query all station relations in Malaysia
    result_relations = api.query("""
    [out:json];
    area["name"="Malaysia"]["boundary"="administrative"]->.searchArea;
    relation["type"="public_transport"](area.searchArea);
    out body;
    > ;
    out skel qt;
    """)
    print(f'Station relation query completed {date.today()}')


    # For each relation, check if it contains any of our entrances
    relationship_id = 0
    for relation in result_relations.relations:
        station_id = relation.tags.get('ref', 'Unnamed')
        station_name = relation.tags.get('name', 'Unnamed')
        for member in relation.members:
            if member.ref in entrances['Entrance ID'].values:
                station_entrances = pd.concat(
                    [station_entrances,
                    pd.DataFrame([{'Relationship ID': relationship_id, 
                                    'Entrance ID': member.ref,
                                    'Station Name': station_name, 
                                    'Station Code': station_id}])], 
                    ignore_index=True
                )
                relationship_id += 1

    # Define the directory where you want to save the cleaned data
    data_directory = 'data'
    kl_entrances_file = 'klang_valley_entrances.csv'
    kl_entrances__station_relations = 'klang_valley_stations_entrances_relation.csv'


    # Save the cleaned dataframes
    entrances.to_csv(os.path.join(data_directory, kl_entrances_file), index=False)
    station_entrances.to_csv(os.path.join(data_directory, kl_entrances__station_relations), index=False)
    logger.info("Querying entrance and station data complete")

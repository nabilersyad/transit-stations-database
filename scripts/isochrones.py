def isoGeoJsonRetriever(parameters,stations,client):

    #ORS isochrones API takes input list of coordinates in field called location. This line creates that column
    stations['locations']  = stations.apply(lambda row: list([row.loc["longitude"],row.loc["latitude"]]) , axis = 1)
    iso_list = []

    for index, row in stations.iterrows():
        print("Retrieving Isochrone of {} station".format(stations.loc[index,'name']))
        parameters['locations'] = [row.loc['locations']]
    
        try:
            temp_iso = client.isochrones(**parameters)
            temp_iso = json.dumps(temp_iso)
            iso_list.append(temp_iso)
            print("Success")
        except Exception as e:
            print(f"Failed to retrieve isochrone for station {stations.loc[index,'name']}: {e}")
            iso_list.append(None)  # or some other default value
    
    stations['iso'] = iso_list

    return

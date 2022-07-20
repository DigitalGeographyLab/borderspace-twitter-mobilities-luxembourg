import sys

# Import from centroid.py
sys.path.insert(0,'../centroid')
from centroid import coordsToPolygon, getCentroid


def fromPlaceToCenterLat(row):
    """
    Function to get latitude of centroid in a row with place information
    
    Parameters
    ----------
    row: pandas row
        Row from dataframe with columns 'place' and 'bounding_box'
    
    Returns
    -------
    center_lat:
        latitude of centroid
    pass:
        if no place information, nothing is returned
    
    """
    if row['place'] != 'None':
        # Convert JSON string to dict
        dictionary = eval(row['place'])
        # Get bounding box coordinates
        try:
            coordList = dictionary['bounding_box']['coordinates']
            # Create polygon
            polygon_geom, lat_point_list, lon_point_list = coordsToPolygon(coordList)
            # Get center coordinates
            center_lon, center_lat = getCentroid(coordList)
            # Return latitude
            return center_lat
        except:
            pass

def fromPlaceToCenterLon(row):
    """
    Function to get longitude of centroid in a row with place information
    
    Parameters
    ----------
    row: pandas row
        Row from dataframe with columns 'place' and 'bounding_box'
    
    Returns
    -------
    center_lon:
        longitude of centroid
    pass:
        if no place information, nothing is returned
    
    """
    if row['place'] != 'None':
        # Convert JSON string to dict
        dictionary = eval(row['place'])
        try:
            # Get bounding box coordinates
            coordList = dictionary['bounding_box']['coordinates']
            # Create polygon
            polygon_geom, lat_point_list, lon_point_list = coordsToPolygon(coordList)
            # Get center coordinates
            center_lon, center_lat = getCentroid(coordList)
            # Return longitude
            return center_lon
        except:
            pass

def fromPlaceToType(row):
    """
    Function to get place type from row with place information
    
    Parameters
    ----------
    row: pandas row
        Row from dataframe with columns 'place' and 'place_type'
    
    Returns
    -------
    place_type:
        string with place type
    pass:
        if no place information, nothing is returned
    
    """
    if row['place'] != 'None':
        # Convert JSON string to dict
        dictionary = eval(row['place'])
        try:
            # Get place type
            place_type = dictionary['place_type']
            return place_type
        except:
            pass


def getLat(row):
    """
    Function to get latitude if a column 'coordinates' with points exists
    
    Parameters
    ----------
    row: pandas row
        Row from dataframe with columns 'coordinates'
    
    Returns
    -------
    lat:
        latitude
    
    """
    if row['coordinates'] != 'None':
        lat = row['coordinates'].split(',')[0][1:]
        return lat

def getLon(row):
    """
    Function to get longitude if a column 'coordinates' with points exists
    
    Parameters
    ----------
    row: pandas row
        Row from dataframe with columns 'coordinates'
    
    Returns
    -------
    lon:
        longitude
    
    """
    if row['coordinates'] != 'None':
        lon = row['coordinates'].split(',')[1][1:-1]
        return lon
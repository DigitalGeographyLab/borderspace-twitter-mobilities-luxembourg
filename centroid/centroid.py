from shapely.geometry import Point, LineString, Polygon


def coordsToPolygon(coordList):
    """
    Function to create a polygon from list of coordinates (Bounding Box)

    Parameters
    ----------
    coordList: list
        List in the style of Twitter json, example:
        [[[4.3139889, 50.7963282], [4.4369472, 50.7963282], [4.4369472, 50.9137064], [4.3139889, 50.9137064]]]

    Returns
    -------
    polygon_geom:
        Polygon
    lat_point_list:
        List of latitude points
    lon_point_list:
        List of longitude points

    """

    # Subset list
    coordList = coordList[0]
    lat_point_list = []
    lon_point_list = []

    for point in coordList:
        # Get longitude
        lon = point[0]
        # Get latitude
        lat = point[1]
        # Append to lists
        lat_point_list.append(lat)
        lon_point_list.append(lon)
    # Create polygon
    polygon_geom = Polygon(zip(lon_point_list, lat_point_list))
    return polygon_geom, lat_point_list, lon_point_list
    

def getCentroid(coordList):
    """
    Function to get center point of polygon.
    
    Parameters
    ----------
    coordList: list
        List in the style of Twitter json, example:
        [[[4.3139889, 50.7963282], [4.4369472, 50.7963282], [4.4369472, 50.9137064], [4.3139889, 50.9137064]]]
    
    Returns
    -------
    center_lon:
        longitude of center point
    center_lat:
        latitude of center point
    
    """
    # Create polygon
    polygon_geom, lat_point_list, lon_point_list = coordsToPolygon(coordList)
    # Get controid
    center = polygon_geom.centroid
    # Get longitude
    center_lon = center.x
    # Get latitude
    center_lat = center.y
    return center_lon, center_lat


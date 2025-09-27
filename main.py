import json
import logging
import math
import os
import requests
from flask import Flask, jsonify
from pyproj import CRS, Transformer
from shapely.geometry import Point
from shapely.ops import transform
from werkzeug.exceptions import BadRequest, InternalServerError

# Setting up basic logging for the Flask app.
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# The API URL for fetching the data.
DATA_API_URL = os.environ.get("DATA_API_URL")
if not DATA_API_URL:
    logger.error("DATA_API_URL environment variable is not set.")
    # In a real app, you might want to handle this more gracefully.
    DATA_API_URL = "http://localhost:8000/data"

app = Flask(__name__)

# Define the coordinate reference systems (CRS) for reprojection.
# WGS84 (EPSG:4326) is the standard for web maps.
WGS84_CRS = CRS("EPSG:4326")
# Greek Grid (EPSG:2100) is a suitable projected system for accurate measurements in Greece.
GREEK_GRID_CRS = CRS("EPSG:2100")

# Create a transformer to convert from WGS84 to Greek Grid.
# The `always_xy=True` ensures the output order is always (longitude, latitude) for WGS84.
transformer_to_greek_grid = Transformer.from_crs(WGS84_CRS, GREEK_GRID_CRS, always_xy=True)
# Create a transformer to convert back from Greek Grid to WGS84.
transformer_to_wgs84 = Transformer.from_crs(GREEK_GRID_CRS, WGS84_CRS, always_xy=True)

@app.route("/no-swim-zones", methods=["GET"])
def get_no_swim_zones():
    """
    Fetches wastewater treatment plant data, creates 200m buffer zones around them
    using accurate coordinate system reprojection, and returns the result as GeoJSON.
    """
    try:
        # Step 1: Fetch data from the external API.
        response = requests.get(DATA_API_URL)
        response.raise_for_status()  # Raise an exception for bad status codes.
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records from the data API.")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from API: {e}")
        raise InternalServerError("Failed to fetch data from the source API.")

    # Step 2: Ensure the data is in the expected GeoJSON format.
    # The API might return a list of features, so we need to handle both cases.
    features = data.get("features", data)
    if not isinstance(features, list):
        logger.error("API response is not a valid GeoJSON FeatureCollection or a list of features.")
        raise BadRequest("Invalid data format from the source API.")

    no_swim_zones = {
        "type": "FeatureCollection",
        "features": []
    }

    # Step 3: Iterate through the features and create buffer zones.
    for feature in features:
        props = feature.get("properties")
        if not props:
            continue

        lat = props.get("Column1.latitude")
        lon = props.get("Column1.longitude")

        # Skip features without valid coordinates.
        if not isinstance(lat, (float, int)) or not isinstance(lon, (float, int)):
            continue

        try:
            # Step 4: Create a Point geometry from the WGS84 coordinates.
            point_wgs84 = Point(lon, lat)

            # Step 5: Reproject the point to the Greek Grid (EPSG:2100).
            # This is the key step to get accurate measurements in meters.
            point_greek_grid = transform(transformer_to_greek_grid.transform, point_wgs84)

            # Step 6: Create a 200-meter buffer around the point in the Greek Grid.
            # Because EPSG:2100 is a projected system, this buffer will be a perfect circle
            # of exactly 200 meters radius.
            buffer_greek_grid = point_greek_grid.buffer(200)

            # Step 7: Reproject the buffer polygon back to WGS84.
            # This is necessary so it can be displayed correctly on a web map.
            buffer_wgs84 = transform(transformer_to_wgs84.transform, buffer_greek_grid)

            # Step 8: Create a new GeoJSON Feature for the buffer zone.
            buffer_feature = {
                "type": "Feature",
                "properties": {
                    "name": props.get("Column1.name"),
                    "nameEn": props.get("Column1.nameEn"),
                    "code": props.get("Column1.code"),
                    "description": "No-swim zone (200m buffer)"
                },
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [list(buffer_wgs84.exterior.coords)]
                }
            }
            no_swim_zones["features"].append(buffer_feature)

        except Exception as e:
            logger.error(f"Error processing feature {props.get('Column1.code', 'N/A')}: {e}")
            # Continue to process other features even if one fails.

    # Step 9: Return the final GeoJSON FeatureCollection.
    return jsonify(no_swim_zones)

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8081)))

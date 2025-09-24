import functions_framework
import requests
import json
import hashlib
from shapely.geometry import Point, mapping
from shapely.ops import cascaded_union
import math
from google.cloud import storage

# A simple helper function to convert meters to degrees at a given latitude.
# This is necessary for accurate buffering with Shapely.
def meters_to_degrees(meters, latitude):
    """
    Converts a distance in meters to degrees of latitude/longitude.
    """
    meters_in_lat_deg = 111132.92 - 559.82 * math.cos(2 * latitude) + 1.175 * math.cos(4 * latitude)
    meters_in_long_deg = 111412.84 * math.cos(latitude) - 93.5 * math.cos(3 * latitude)
    return meters / meters_in_lat_deg, meters / meters_in_long_deg

# Constants for the Cloud Function.
# You need to replace these with your actual GCS bucket and file names.
WASTEWATER_API_URL = "https://astikalimata.ypeka.gr/api/query/wastewatertreatmentplants"
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
PERIFEREIES_GEOJSON_PATH = "perifereiesWGS84.geojson"
LAST_HASH_FILE_PATH = "wastewater_data_hash.txt"
OUTPUT_GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"
BUFFER_DISTANCE_METERS = 200

def get_gcs_blob(bucket_name, blob_name):
    """Retrieves a blob from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket.blob(blob_name)

def load_perifereies_data(bucket_name, file_path):
    """Loads and parses the large perifereies GeoJSON file from GCS."""
    try:
        blob = get_gcs_blob(bucket_name, file_path)
        geojson_data = blob.download_as_text()
        perifereies_features = json.loads(geojson_data)
        perifereies_geometries = [
            Point(f['geometry']['coordinates']) if f['geometry']['type'] == 'Point' else
            f['geometry'] for f in perifereies_features['features']
        ]
        return perifereies_geometries
    except Exception as e:
        print(f"Error loading perifereies GeoJSON: {e}")
        return None

def calculate_new_zones(perifereies_geometries, wastewater_data):
    """
    Performs the core geospatial analysis: buffering, union, and difference.
    """
    print("Starting geospatial analysis...")
    all_buffers = []
    
    for plant_feature in wastewater_data['features']:
        try:
            # Use .get() with a fallback to avoid KeyErrors
            props = plant_feature['properties']
            longitude = props.get('Column1.receiverLocation.1')
            latitude = props.get('Column1.receiverLocation.2')

            # Fallback to the regular latitude/longitude keys if receiverLocation keys are missing or null
            if longitude is None or latitude is None:
                longitude = props.get('Column1.longitude')
                latitude = props.get('Column1.latitude')

            # Skip the plant if no valid coordinates are found
            if longitude is None or latitude is None:
                print(f"Skipping plant '{props.get('Column1.name')}' due to missing coordinates.")
                continue
                
            # Create a Point geometry from the coordinates
            point = Point(longitude, latitude)

            # Convert 200m buffer distance to degrees at the given latitude
            # We'll use the latitude conversion for both axes for simplicity
            buffer_lat_deg, buffer_lon_deg = meters_to_degrees(BUFFER_DISTANCE_METERS, math.radians(latitude))
            buffer_radius = buffer_lat_deg # Averages a reasonable buffer in degrees
            
            # Create the 200m buffer around the point
            buffered_point = point.buffer(buffer_radius)
            all_buffers.append(buffered_point)
        except KeyError as e:
            print(f"Skipping plant due to missing coordinate: {e}")
            continue

    # Union all the individual buffers into a single MultiPolygon
    if not all_buffers:
        print("No valid wastewater points found. Exiting.")
        return None
        
    unified_buffers = cascaded_union(all_buffers)

    # Union the perifereies geometries for the difference calculation
    unified_perifereies = cascaded_union(perifereies_geometries)
    
    # Calculate the difference to find danger zones outside the mainland
    # The result is a new geometry representing the no-swim zones.
    danger_zones = unified_buffers.difference(unified_perifereies)
    
    print("Geospatial analysis complete.")
    return danger_zones

@functions_framework.http
def check_for_changes(request):
    """
    Main entry point for the Google Cloud Function.
    Fetches wastewater data, checks for changes, and if found,
    recalculates and updates the no-swimming zones.
    """
    print("Function started.")
    
    # 1. Fetch the latest wastewater data
    try:
        response = requests.get(WASTEWATER_API_URL, timeout=30)
        response.raise_for_status()
        wastewater_data = response.json()
        current_data_string = json.dumps(wastewater_data, sort_keys=True)
        current_hash = hashlib.sha256(current_data_string.encode('utf-8')).hexdigest()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
        return ("Failed to fetch data.", 500)
    
    # 2. Compare with the last known hash from GCS
    try:
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        if hash_blob.exists():
            last_hash = hash_blob.download_as_text()
            if current_hash == last_hash:
                print("No changes detected in wastewater data. Exiting.")
                return ("No changes detected.", 200)
        else:
            print("No previous hash found. Proceeding with analysis.")
    except Exception as e:
        print(f"Error checking last hash: {e}")
        # Continue anyway in case of an error accessing GCS
        
    # 3. If changes detected, perform geospatial analysis
    print("Changes detected. Starting geospatial analysis...")
    
    # Load the large Greek perifereies GeoJSON from GCS
    perifereies_geometries = load_perifereies_data(GCS_BUCKET_NAME, PERIFEREIES_GEOJSON_PATH)
    if perifereies_geometries is None:
        return ("Failed to load perifereies data.", 500)
        
    # Calculate the new no-swimming zones
    danger_zones_geometry = calculate_new_zones(perifereies_geometries, wastewater_data)
    
    if danger_zones_geometry is None:
        return ("Analysis failed.", 500)

    # 4. Save the new zones and update the hash in GCS
    try:
        # Create a GeoJSON FeatureCollection from the resulting geometry
        new_zones_geojson = {
            "type": "FeatureCollection",
            "features": [
                {
                    "type": "Feature",
                    "geometry": mapping(danger_zones_geometry),
                    "properties": {}
                }
            ]
        }
        
        output_blob = get_gcs_blob(GCS_BUCKET_NAME, OUTPUT_GEOJSON_PATH)
        output_blob.upload_from_string(
            json.dumps(new_zones_geojson),
            content_type="application/geo+json"
        )
        print(f"New no-swimming zones saved to GCS at gs://{GCS_BUCKET_NAME}/{OUTPUT_GEOJSON_PATH}")

        # Update the hash file for the next run
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        print("Hash file updated. Operation successful.")
        
    except Exception as e:
        print(f"Failed to save results to GCS: {e}")
        return ("Failed to save results.", 500)
        
    return ("Analysis complete. New zones calculated and saved.", 200)

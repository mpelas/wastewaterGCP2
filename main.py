import functions_framework
import requests
import json
import hashlib
from shapely.geometry import Point, mapping, shape
from shapely.ops import unary_union
import math
from google.cloud import storage
from shapely import wkt
import copy # Import copy module for clean metadata handling

# A simple helper function to convert meters to degrees at a given latitude.
# This is necessary for accurate buffering with Shapely.
def meters_to_degrees(meters, latitude):
    """
    Converts a distance in meters to degrees of latitude/longitude.
    """
    # Note: math.cos expects radians, so latitude must be converted to radians before use
    lat_rad = math.radians(latitude)
    meters_in_lat_deg = 111132.92 - 559.82 * math.cos(2 * lat_rad) + 1.175 * math.cos(4 * lat_rad)
    meters_in_long_deg = 111412.84 * math.cos(lat_rad) - 93.5 * math.cos(3 * lat_rad) * math.cos(lat_rad)
    return meters / meters_in_lat_deg, meters / meters_in_long_deg

# Constants for the Cloud Function.
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
        # Convert GeoJSON geometries to Shapely geometry objects
        perifereies_geometries = [
            shape(f['geometry']) for f in perifereies_features['features']
        ]
        print("====DIABASA tis perifereies perifereiesWGS84.geojson")
        return perifereies_geometries
    except Exception as e:
        print(f"Error loading perifereies GeoJSON: {e}")
        return None

def calculate_new_zones(perifereies_geometries, wastewater_data):
    """
    Performs the core geospatial analysis: buffering, difference, and retention of ALL metadata.
    Returns a list of tuples: (Shapely Geometry, ALL Metadata Dictionary).
    """
    print("Starting geospatial analysis with full metadata retention...")
    
    # Union the perifereies geometries once for efficient difference calculation
    unified_perifereies = unary_union(perifereies_geometries)
    
    no_swim_zones_with_metadata = []

    # Check data format
    if isinstance(wastewater_data, dict) and 'features' in wastewater_data:
        features_to_process = wastewater_data['features']
    elif isinstance(wastewater_data, list):
        features_to_process = wastewater_data
    else:
        print("Invalid wastewater data format.")
        return None

    for plant_feature in features_to_process:
        try:
            # Extract the metadata dictionary (either from 'properties' or the top level)
            props = plant_feature.get('properties', plant_feature)
            
            # --- Metadata Handling ---
            # Copy ALL properties to use as metadata for the final GeoJSON feature
            # The .copy() ensures we don't modify the source data
            metadata = copy.copy(props)

            # --- Location Logic (Priority: receiverLocation WKT > Plant Lat/Lon) ---
            receiver_location_wkt = props.get('receiverLocation')
            longitude = props.get('longitude')
            latitude = props.get('latitude')

            point = None
            if receiver_location_wkt:
                try:
                    # Priority 1: Use WKT (receiver location)
                    point = wkt.loads(receiver_location_wkt)
                except Exception as e:
                    print(f"Error parsing WKT for plant '{props.get('name')}': {e}. Falling back to main coordinates.")
            
            if point is None:
                # Priority 2: Fallback to main plant coordinates
                if longitude is not None and latitude is not None:
                    point = Point(longitude, latitude)
                    # Update metadata to reflect the source of coordinates if necessary, though the original lat/lon are already there.
                    print(f"Using main plant coordinates for '{props.get('name')}' as receiverLocation is missing/invalid.")
                else:
                    print(f"Skipping plant '{props.get('name')}' due to missing coordinates.")
                    continue

            # --- Geospatial Processing ---
            
            # Convert 200m buffer distance to degrees at the given latitude
            buffer_lat_deg, buffer_lon_deg = meters_to_degrees(BUFFER_DISTANCE_METERS, point.y)
            # Use the latitude degree equivalent as the average radius for the buffer
            buffer_radius = buffer_lat_deg 

            buffered_point = point.buffer(buffer_radius)
            
            # Perform Difference: Find the part of the buffer that is *not* on the mainland
            danger_zone = buffered_point.difference(unified_perifereies)
            
            # If the difference results in a valid maritime geometry (i.e., not empty), save it
            if not danger_zone.is_empty:
                no_swim_zones_with_metadata.append((danger_zone, metadata))
                
        except Exception as e:
            print(f"Skipping plant due to an unhandled error processing its data: {e}")
            continue

    print(f"Geospatial analysis complete. Found {len(no_swim_zones_with_metadata)} no-swim zones.")
    return no_swim_zones_with_metadata

# --- EXECUTION FLOW ---
@functions_framework.http
def check_for_changes(request):
    """
    Main entry point for the Google Cloud Function.
    Fetches wastewater data, checks for changes, and if found,
    recalculates and updates the no-swimming zones with ALL metadata.
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

    # Calculate the new no-swimming zones (Geometry, ALL Metadata)
    zones_list = calculate_new_zones(perifereies_geometries, wastewater_data)

    if not zones_list:
        print("Analysis completed, but no maritime danger zones were found.")
        # Create an empty GeoJSON file
        empty_geojson = {"type": "FeatureCollection", "features": []}
        output_blob = get_gcs_blob(GCS_BUCKET_NAME, OUTPUT_GEOJSON_PATH)
        output_blob.upload_from_string(json.dumps(empty_geojson), content_type="application/geo+json")
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        return ("Analysis complete. No zones found and an empty file was saved.", 200)

    # 4. Save the new zones and update the hash in GCS
    try:
        # Construct the GeoJSON FeatureCollection with ALL metadata for each feature
        geojson_features = []
        for geometry, metadata in zones_list:
            geojson_features.append({
                "type": "Feature",
                # The geometry can be a Polygon or MultiPolygon depending on the difference result
                "geometry": mapping(geometry), 
                "properties": metadata
            })

        new_zones_geojson = {
            "type": "FeatureCollection",
            "features": geojson_features
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

    return ("Analysis complete. New zones calculated and saved with full metadata.", 200)

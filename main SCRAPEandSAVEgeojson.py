import functions_framework
import requests
import json
import hashlib
from shapely.geometry import Point, mapping, shape
from shapely.ops import cascaded_union, transform
from shapely import wkt  # <-- ADDED: Need this to parse WKT strings
from google.cloud import storage
from pyproj import CRS, Transformer
import math

# Constants for the Cloud Function.
WASTEWATER_API_URL = "https://astikalimata.ypeka.gr/api/query/wastewatertreatmentplants"
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
PERIFEREIES_GEOJSON_PATH = "perifereiesWGS84.geojson"
LAST_HASH_FILE_PATH = "wastewater_data_hash.txt"
OUTPUT_GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"
BUFFER_DISTANCE_METERS = 200

# Define coordinate reference systems
WGS84_CRS = CRS("EPSG:4326")      # Standard GPS coordinates (Degrees)
GREEK_GRID_CRS = CRS("EPSG:2100")  # Greek Grid for accurate meters (Meters)

# Create transformers (always_xy=True ensures correct (lon, lat) or (east, north) order)
transformer_to_greek_grid = Transformer.from_crs(WGS84_CRS, GREEK_GRID_CRS, always_xy=True).transform
transformer_to_wgs84 = Transformer.from_crs(GREEK_GRID_CRS, WGS84_CRS, always_xy=True).transform

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
            shape(f['geometry']) for f in perifereies_features.get('features', [])
        ]
        print("====DIABASA tis perifereies perifereiesWGS84.geojson")
        print(perifereies_geometries)
        return perifereies_geometries
    except Exception as e:
        print(f"Error loading perifereies GeoJSON: {e}")
        return None

def calculate_new_zones(perifereies_geometries, wastewater_data):
    """
    Performs the core geospatial analysis: buffering, union, and difference.
    """
    print("Starting geospatial analysis...")
    no_swim_zones_with_metadata = [] # Renamed from original no_swim_zones_with_metadata list to hold GeoJSON features

    # --- CRITICAL FIX 1: UNIFY PERIPHEREIES ---
    if not perifereies_geometries:
        print("Perifereies geometries are empty. Cannot calculate differences.")
        return []
    
    # Create the single, unified geometry for the difference operation
    unified_perifereies = cascaded_union(perifereies_geometries)
    print("Perifereies unified successfully.")

    # Check if the data is a FeatureCollection or a list of features
    if isinstance(wastewater_data, dict) and 'features' in wastewater_data:
        features_to_process = wastewater_data['features']
    elif isinstance(wastewater_data, list):
        features_to_process = wastewater_data
    else:
        print("Invalid wastewater data format.")
        return []

    print("XXXXXXXXXXXXX features_to_process=")
    print(features_to_process)
    
    for plant_feature in features_to_process:
        try:
            # Assume 'properties' key is present as is common in GeoJSON structure
            props = plant_feature.get('properties', plant_feature) # Fallback handling kept for safety

            # Prepare metadata for the final GeoJSON feature
            metadata = {
                'code': props.get('code'),
                'name': props.get('name'),
                'receiverName': props.get('receiverName'),
                'receiverNameEn': props.get('receiverNameEn'),
                'receiverWaterType': props.get('receiverWaterType'),
                'latitude': props.get('latitude'),
                'longitude': props.get('longitude')
            }

            receiver_location_wkt = props.get('receiverLocation')
            longitude = props.get('longitude')
            latitude = props.get('latitude')

            point_wgs84 = None
            
            # 1. Determine the discharge point (WKT preferred, then lat/lon)
            if receiver_location_wkt:
                try:
                    point_wgs84 = wkt.loads(receiver_location_wkt)
                except Exception as e:
                    print(f"Error parsing WKT for plant '{metadata.get('name')}': {e}. Falling back to main coordinates.")
            
            if point_wgs84 is None and longitude is not None and latitude is not None:
                point_wgs84 = Point(longitude, latitude)
            
            if point_wgs84 is None or point_wgs84.is_empty:
                print(f"Skipping plant '{metadata.get('name')}' due to missing or invalid coordinates.")
                continue

            print("name=", metadata.get('name'))
            print("longitude=", point_wgs84.x)
            print("latitude=", point_wgs84.y)
            
            # --- CRITICAL FIX 2: Reprojection for Accurate Buffering ---
            
            # 2. Project the WGS84 point to the metric Greek Grid (EPSG:2100)
            point_greek_grid = transform(transformer_to_greek_grid, point_wgs84)

            # 3. Buffer the point in meters
            buffered_point_greek_grid = point_greek_grid.buffer(BUFFER_DISTANCE_METERS)
            
            # 4. Project the buffer back to WGS84 
            buffered_point_wgs84 = transform(transformer_to_wgs84, buffered_point_greek_grid)
            
            # 5. Perform Difference: Find the part of the buffer that is *not* on the mainland
            danger_zone = buffered_point_wgs84.difference(unified_perifereies)
            
            # If the difference results in a valid geometry (i.e., not empty), save it
            if not danger_zone.is_empty:
                # Store as a GeoJSON Feature object
                no_swim_zones_with_metadata.append({
                    "type": "Feature",
                    "geometry": mapping(danger_zone),
                    "properties": metadata
                })
        
        except Exception as e:
            # Catch and log any other unexpected processing errors for a single plant
            print(f"Skipping plant due to an error processing its data: {e}")
            continue

    print(f"Geospatial analysis complete. Found {len(no_swim_zones_with_metadata)} no-swim zones.")
    return no_swim_zones_with_metadata

# ----------------------------------------------------------------------
# --- EXECUTION FLOW MODIFIED HERE ---
@functions_framework.http
def check_for_changes(request):
    """
    Main entry point for the Google Cloud Function.
    Fetches wastewater data, checks for changes, and if found,
    recalculates and updates the no-swimming zones with metadata.
    """
    print("Function started.")
    
    # 1. Fetch wastewater data
    try:
        response = requests.get(WASTEWATER_API_URL, timeout=30)
        response.raise_for_status()
        wastewater_data = response.json()
        current_data_string = json.dumps(wastewater_data, sort_keys=True)
        current_hash = hashlib.sha256(current_data_string.encode('utf-8')).hexdigest()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
        return ("Failed to fetch data.", 500)
        
    # 2. Compare hash
    try:
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        if hash_blob.exists():
            last_hash = hash_blob.download_as_text()
            if current_hash == last_hash:
                print("No changes detected. Exiting.")
                return ("No changes detected.", 200)
        else:
            print("No previous hash found. Proceeding with analysis.")
    except Exception as e:
        print(f"Error checking last hash: {e}. Proceeding with analysis.") # Proceed if hash check fails
        
    # 3. Load perifereies data
    perifereies_geometries = load_perifereies_data(GCS_BUCKET_NAME, PERIFEREIES_GEOJSON_PATH)
    if perifereies_geometries is None:
        return ("Failed to load perifereies data.", 500)
        
    # 4. Calculate danger zones (returns a list of GeoJSON Feature objects)
    new_zones_features = calculate_new_zones(perifereies_geometries, wastewater_data)
    
    # NOTE: The original code used danger_zones_geometry which was incorrect because it implied a single geometry.
    # The variable is now correctly named new_zones_features, which is a list of features.

    if not new_zones_features:
        print("Analysis resulted in no new zones to save.")
        return ("Analysis complete. No zones saved.", 200)
        
    # 5. Save results
    try:
        # Construct the final FeatureCollection
        new_zones_geojson = {
            "type": "FeatureCollection",
            "features": new_zones_features  # List of GeoJSON Feature objects
        }
        
        output_blob = get_gcs_blob(GCS_BUCKET_NAME, OUTPUT_GEOJSON_PATH)
        output_blob.upload_from_string(
            json.dumps(new_zones_geojson),
            content_type="application/geo+json"
        )
        print(f"Saved to GCS: gs://{GCS_BUCKET_NAME}/{OUTPUT_GEOJSON_PATH}")
        
        # Update hash
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        print("Hash file updated.")
        
    except Exception as e:
        print(f"Failed to save results: {e}")
        return ("Failed to save results.", 500)
        
    return ("Analysis complete. New zones saved.", 200)
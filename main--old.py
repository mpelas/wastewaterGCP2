import functions_framework
import requests
import json
import hashlib
from shapely.geometry import Point, mapping, shape
from shapely.ops import unary_union
import math
from google.cloud import storage
from pyproj import CRS, Transformer

# Constants for the Cloud Function.
WASTEWATER_API_URL = "https://astikalimata.ypeka.gr/api/query/wastewatertreatmentplants"
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
PERIFEREIES_GEOJSON_PATH = "perifereiesWGS84.geojson"
LAST_HASH_FILE_PATH = "wastewater_data_hash.txt"
OUTPUT_GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"
BUFFER_DISTANCE_METERS = 200

# Define coordinate reference systems
WGS84_CRS = CRS("EPSG:4326")  # Standard GPS coordinates
GREEK_GRID_CRS = CRS("EPSG:2100")  # Greek Grid for accurate meters

# Create transformers
transformer_to_greek_grid = Transformer.from_crs(WGS84_CRS, GREEK_GRID_CRS, always_xy=True)
transformer_to_wgs84 = Transformer.from_crs(GREEK_GRID_CRS, WGS84_CRS, always_xy=True)

def get_gcs_blob(bucket_name, blob_name):
    """Retrieves a blob from Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    return bucket.blob(blob_name)

def load_perifereies_data(bucket_name, file_path):
    """Loads and parses the perifereies GeoJSON file from GCS."""
    try:
        blob = get_gcs_blob(bucket_name, file_path)
        geojson_data = blob.download_as_text()
        perifereies_features = json.loads(geojson_data)
        # Convert GeoJSON geometries to Shapely geometry objects
        perifereies_geometries = [
            shape(f['geometry']) for f in perifereies_features['features']
        ]
        print("Loaded perifereies GeoJSON.")
        return perifereies_geometries
    except Exception as e:
        print(f"Error loading perifereies GeoJSON: {e}")
        return None

def calculate_new_zones(perifereies_geometries, wastewater_data):
    """
    Performs the core geospatial analysis: buffering, union, and difference.
    Uses Greek Grid (EPSG:2100) for accurate 200m circular buffers.
    """
    print("Starting geospatial analysis with Greek Grid reprojection...")
    all_buffers = []

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
            # Extract properties
            props = plant_feature.get('properties', plant_feature)

            # Get coordinates (prioritize receiverLocation, fallback to latitude/longitude)
            longitude = props.get('Column1.receiverLocation.1')
            latitude = props.get('Column1.receiverLocation.2')
            if longitude is None or latitude is None:
                longitude = props.get('Column1.longitude')
                latitude = props.get('Column1.latitude')

            # Skip if no valid coordinates
            if longitude is None or latitude is None:
                print(f"Skipping plant '{props.get('Column1.name')}' due to missing coordinates.")
                continue

            # Create WGS84 point
            point_wgs84 = Point(longitude, latitude)

            # Reproject to Greek Grid
            x, y = transformer_to_greek_grid.transform(point_wgs84.x, point_wgs84.y)
            point_greek_grid = Point(x, y)

            # Create 200m circular buffer in Greek Grid (meters)
            buffer_greek_grid = point_greek_grid.buffer(BUFFER_DISTANCE_METERS)

            # Reproject buffer back to WGS84
            buffer_wgs84 = transform(transformer_to_wgs84.transform, buffer_greek_grid)

            all_buffers.append(buffer_wgs84)

        except Exception as e:
            print(f"Skipping plant due to an error: {e}")
            continue

    # Union all buffers
    if not all_buffers:
        print("No valid wastewater points found. Exiting.")
        return None

    unified_buffers = unary_union(all_buffers)

    # Union perifereies geometries
    unified_perifereies = unary_union(perifereies_geometries)

    # Calculate danger zones (buffers not on mainland)
    danger_zones = unified_buffers.difference(unified_perifereies)

    print("Geospatial analysis complete.")
    return danger_zones

@functions_framework.http
def check_for_changes(request):
    """
    Main entry point for the Google Cloud Function.
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
        print(f"Error checking last hash: {e}")

    # 3. Load perifereies data
    perifereies_geometries = load_perifereies_data(GCS_BUCKET_NAME, PERIFEREIES_GEOJSON_PATH)
    if perifereies_geometries is None:
        return ("Failed to load perifereies data.", 500)

    # 4. Calculate danger zones
    danger_zones_geometry = calculate_new_zones(perifereies_geometries, wastewater_data)
    if danger_zones_geometry is None:
        return ("Analysis failed.", 500)

    # 5. Save results
    try:
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
        print(f"Saved to GCS: gs://{GCS_BUCKET_NAME}/{OUTPUT_GEOJSON_PATH}")

        # Update hash
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        print("Hash file updated.")

    except Exception as e:
        print(f"Failed to save results: {e}")
        return ("Failed to save results.", 500)

    return ("Analysis complete. New zones saved.", 200)

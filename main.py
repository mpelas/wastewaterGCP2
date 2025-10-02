import functions_framework
import requests
import json
import hashlib
import io
import simplekml # For KML generation
from shapely.geometry import Point, mapping, shape
from shapely.ops import cascaded_union, transform
from shapely import wkt
from google.cloud import storage
from pyproj import CRS, Transformer
import math
# Imports for Google Drive
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from flask import jsonify

# ======================================================================
# --- CONSTANTS ---
# ======================================================================

# Constants for the main Cloud Function (check_for_changes)
WASTEWATER_API_URL = "https://astikalimata.ypeka.gr/api/query/wastewatertreatmentplants"
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
PERIFEREIES_GEOJSON_PATH = "perifereiesWGS84.geojson"
LAST_HASH_FILE_PATH = "wastewater_data_hash.txt"
OUTPUT_GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"
BUFFER_DISTANCE_METERS = 200

# Constants for the KML/Drive Sync (sync_to_drive logic)
GEOJSON_PATH = OUTPUT_GEOJSON_PATH # Same file path
DRIVE_FOLDER_ID = "122jxF5nlwH8Re3ixoCjf2TuHyNCDuuxD"
SCOPES = ['https://www.googleapis.com/auth/drive.file']

# Define coordinate reference systems
WGS84_CRS = CRS("EPSG:4326")        # Standard GPS coordinates (Degrees)
GREEK_GRID_CRS = CRS("EPSG:2100")  # Greek Grid for accurate meters (Meters)

# Create transformers (always_xy=True ensures correct (lon, lat) or (east, north) order)
transformer_to_greek_grid = Transformer.from_crs(WGS84_CRS, GREEK_GRID_CRS, always_xy=True).transform
transformer_to_wgs84 = Transformer.from_crs(GREEK_GRID_CRS, WGS84_CRS, always_xy=True).transform

# ======================================================================
# --- HELPER FUNCTIONS (GCS/Geospatial) ---
# ======================================================================

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
        # print(perifereies_geometries) # Commented out for cleaner logs
        return perifereies_geometries
    except Exception as e:
        print(f"Error loading perifereies GeoJSON: {e}")
        return None

def calculate_new_zones(perifereies_geometries, wastewater_data):
    """
    Performs the core geospatial analysis: buffering, union, and difference.
    Returns a list of GeoJSON features.
    """
    print("Starting geospatial analysis...")
    no_swim_zones_with_metadata = []

    if not perifereies_geometries:
        print("Perifereies geometries are empty. Cannot calculate differences.")
        return []

    # Create the single, unified geometry for the difference operation
    unified_perifereies = cascaded_union(perifereies_geometries)
    print("Perifereies unified successfully.")

    if isinstance(wastewater_data, dict) and 'features' in wastewater_data:
        features_to_process = wastewater_data['features']
    elif isinstance(wastewater_data, list):
        features_to_process = wastewater_data
    else:
        print("Invalid wastewater data format.")
        return []

    for plant_feature in features_to_process:
        try:
            props = plant_feature.get('properties', plant_feature)
            
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
            
            # 1. Determine the discharge point
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

            # 2. Project the WGS84 point to the metric Greek Grid (EPSG:2100)
            point_greek_grid = transform(transformer_to_greek_grid, point_wgs84)

            # 3. Buffer the point in meters
            buffered_point_greek_grid = point_greek_grid.buffer(BUFFER_DISTANCE_METERS)
            
            # 4. Project the buffer back to WGS84 
            buffered_point_wgs84 = transform(transformer_to_wgs84, buffered_point_greek_grid)
            
            # 5. Perform Difference: Find the part of the buffer that is *not* on the mainland
            danger_zone = buffered_point_wgs84.difference(unified_perifereies)
            
            if not danger_zone.is_empty:
                # Store as a GeoJSON Feature object
                # Adding 'location' and 'compliance' keys for KML conversion compatibility
                kml_properties = {
                    'location': metadata.get('name', 'Unknown Location'),
                    'Column1.compliance': props.get('is_compliant', True), # Assuming a 'is_compliant' key or defaulting to True
                    'details': f"Code: {metadata.get('code', 'N/A')}. Receiver: {metadata.get('receiverName', 'N/A')}",
                    **metadata
                }
                no_swim_zones_with_metadata.append({
                    "type": "Feature",
                    "geometry": mapping(danger_zone),
                    "properties": kml_properties
                })
        
        except Exception as e:
            print(f"Skipping plant due to an error processing its data: {e}")
            continue

    print(f"Geospatial analysis complete. Found {len(no_swim_zones_with_metadata)} no-swim zones.")
    return no_swim_zones_with_metadata

# ======================================================================
# --- HELPER FUNCTIONS (KML/Drive) ---
# ======================================================================

def get_geojson_from_gcs():
    """Download GeoJSON from GCS"""
    storage_client = storage.Client()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob = bucket.blob(GEOJSON_PATH)
    geojson_data = json.loads(blob.download_as_text())
    return geojson_data

def geojson_to_kml(geojson_data):
    """Convert GeoJSON to KML format"""
    kml = simplekml.Kml()
    
    for feature in geojson_data.get('features', []):
        geometry = feature.get('geometry', {})
        properties = feature.get('properties', {})
        geom_type = geometry.get('type')
        coordinates = geometry.get('coordinates', [])
        
        # Get properties for styling and info (using the enhanced properties saved in calculate_new_zones)
        location = properties.get('location', 'Unknown Location')
        compliance = properties.get('Column1.compliance', None)
        details = properties.get('details', 'No details available')
        
        # Create description
        description = f"""
        <![CDATA[
        <b>Name:</b> {properties.get('name', 'N/A')}<br>
        <b>Code:</b> {properties.get('code', 'N/A')}<br>
        <b>Receiver:</b> {properties.get('receiverName', 'N/A')}<br>
        <b>Compliance:</b> {'⚠️ NON-COMPLIANT' if compliance is False else '✓ Compliant'}<br>
        <b>Details:</b> {details}
        ]]>
        """
        
        # Determine color based on compliance
        if compliance is False:
            color = simplekml.Color.red
        else:
            # We'll use a slightly different color for zones to differentiate from points if needed, e.g., Semi-Transparent Blue
            color = simplekml.Color.changealphaint(150, simplekml.Color.blue) 
        
        # Add geometry to KML
        if geom_type == 'Polygon':
            coords = coordinates[0]
            kml_coords = [(coord[0], coord[1]) for coord in coords]
            
            pol = kml.newpolygon(name=location, description=description)
            pol.outerboundaryis = kml_coords
            pol.style.polystyle.color = color
            pol.style.polystyle.fill = 1
            pol.style.polystyle.outline = 1
            pol.style.linestyle.color = simplekml.Color.white
            pol.style.linestyle.width = 2
            
        elif geom_type == 'MultiPolygon':
            for i, polygon in enumerate(coordinates):
                coords = polygon[0]
                kml_coords = [(coord[0], coord[1]) for coord in coords]
                
                pol = kml.newpolygon(name=f"{location} Part {i+1}", description=description)
                pol.outerboundaryis = kml_coords
                pol.style.polystyle.color = color
                pol.style.polystyle.fill = 1
                pol.style.polystyle.outline = 1
                pol.style.linestyle.color = simplekml.Color.white
                pol.style.linestyle.width = 2
            
    # Generate KML string
    kml_string = kml.kml()
    return kml_string

def upload_to_drive(file_content, filename, folder_id=None):
    """Upload file to Google Drive. Updates if file exists."""
    try:
        # Use default credentials (Cloud Function service account)
        credentials = service_account.Credentials.from_service_account_info(
            info={},
            scopes=SCOPES
        )
        
        # Build Drive service
        service = build('drive', 'v3', credentials=credentials)
        
        # Prepare file metadata
        file_metadata = {
            'name': filename,
            'mimeType': 'application/vnd.google-earth.kml+xml'
        }
        
        if folder_id:
            file_metadata['parents'] = [folder_id]
        
        # Create file in memory
        fh = io.BytesIO(file_content.encode('utf-8'))
        media = MediaIoBaseUpload(
            fh,
            mimetype='application/vnd.google-earth.kml+xml',
            resumable=True
        )
        
        # Check if file already exists
        query = f"name='{filename}' and trashed=false"
        if folder_id:
            query += f" and '{folder_id}' in parents"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        existing_files = results.get('files', [])
        
        if existing_files:
            # Update existing file
            file_id = existing_files[0]['id']
            file = service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
            action = 'updated'
        else:
            # Create new file
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id, webViewLink'
            ).execute()
            action = 'created'
        
        # Make file accessible (optional, but good for sharing)
        permission = {
            'type': 'anyone',
            'role': 'reader'
        }
        service.permissions().create(
            fileId=file.get('id'),
            body=permission
        ).execute()
        
        return {
            'file_id': file.get('id'),
            'web_link': file.get('webViewLink', f"https://drive.google.com/file/d/{file.get('id')}/view"),
            'action': action
        }
        
    except Exception as e:
        print(f"Error uploading to Drive: {e}")
        raise

# ======================================================================
# --- MAIN WORKFLOW FUNCTIONS ---
# ======================================================================

def sync_to_drive_internal():
    """Internal function to handle KML conversion and Drive upload."""
    print("==== Starting GeoJSON to Drive sync ====")
    
    # 1. Download GeoJSON from GCS
    print(f"Downloading GeoJSON from gs://{GCS_BUCKET_NAME}/{GEOJSON_PATH}")
    geojson_data = get_geojson_from_gcs()
    feature_count = len(geojson_data.get('features', []))
    print(f"Loaded {feature_count} features")
    
    # 2. Convert to KML
    print("Converting GeoJSON to KML...")
    kml_content = geojson_to_kml(geojson_data)
    print(f"KML generated, size: {len(kml_content)} bytes")
    
    # 3. Upload to Drive
    filename = "wastewater_no_swim_zones.kml"
    print(f"Uploading to Google Drive as '{filename}'...")
    drive_result = upload_to_drive(kml_content, filename, DRIVE_FOLDER_ID)
    
    print(f"==== Sync complete: {drive_result['action']} ====")
    
    return {
        'success': True,
        'message': f'Successfully {drive_result["action"]} KML file in Google Drive',
        'feature_count': feature_count,
        'drive_file_id': drive_result['file_id'],
        'drive_link': drive_result['web_link'],
        'filename': filename
    }


@functions_framework.http
def check_for_changes(request):
    """
    Main Cloud Function entry point.
    1. Fetches wastewater data and checks for changes.
    2. If changes exist, recalculates and updates the GeoJSON in GCS.
    3. Calls the KML sync process.
    """
    print("Function started: check_for_changes.")
    
    # --- Part 1: Fetch and Check Hash ---
    try:
        response = requests.get(WASTEWATER_API_URL, timeout=30)
        response.raise_for_status()
        wastewater_data = response.json()
        current_data_string = json.dumps(wastewater_data, sort_keys=True)
        current_hash = hashlib.sha256(current_data_string.encode('utf-8')).hexdigest()
    except requests.exceptions.RequestException as e:
        print(f"Failed to fetch data from API: {e}")
        return ("Failed to fetch data.", 500)
        
    try:
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        if hash_blob.exists():
            last_hash = hash_blob.download_as_text()
            if current_hash == last_hash:
                print("No changes detected in wastewater data. Checking Drive sync...")
                # Even if no changes, we call the sync to ensure the KML file exists and is up-to-date
                try:
                    drive_result = sync_to_drive_internal()
                    return (f"No data changes. KML Drive sync: {drive_result['action']}.", 200)
                except Exception as e:
                    return (f"No data changes. KML Drive sync failed: {str(e)}", 500)
        else:
            print("No previous hash found. Proceeding with analysis.")
    except Exception as e:
        print(f"Error checking last hash: {e}. Proceeding with analysis.")
        
    # --- Part 2: Load, Calculate, and Save GeoJSON ---
    
    perifereies_geometries = load_perifereies_data(GCS_BUCKET_NAME, PERIFEREIES_GEOJSON_PATH)
    if perifereies_geometries is None:
        return ("Failed to load perifereies data.", 500)
        
    new_zones_features = calculate_new_zones(perifereies_geometries, wastewater_data)
    
    if not new_zones_features:
        print("Analysis resulted in no new zones to save. Updating hash to prevent immediate re-run.")
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        return ("Analysis complete. No zones saved.", 200)
        
    try:
        # Construct the final FeatureCollection
        new_zones_geojson = {
            "type": "FeatureCollection",
            "features": new_zones_features
        }
        
        # Save GeoJSON
        output_blob = get_gcs_blob(GCS_BUCKET_NAME, OUTPUT_GEOJSON_PATH)
        output_blob.upload_from_string(
            json.dumps(new_zones_geojson),
            content_type="application/geo+json"
        )
        print(f"Saved new GeoJSON to GCS: gs://{GCS_BUCKET_NAME}/{OUTPUT_GEOJSON_PATH}")
        
        # Update hash
        hash_blob = get_gcs_blob(GCS_BUCKET_NAME, LAST_HASH_FILE_PATH)
        hash_blob.upload_from_string(current_hash)
        print("Hash file updated.")
        
    except Exception as e:
        print(f"Failed to save results to GCS: {e}")
        return ("Failed to save GeoJSON results.", 500)
        
    # --- Part 3: KML Conversion and Drive Upload ---
    try:
        drive_result = sync_to_drive_internal()
        
        return (f"Analysis complete. New GeoJSON saved. KML Drive sync: {drive_result['action']}.", 200)
        
    except Exception as e:
        print(f"KML Sync to Drive Failed: {e}")
        # Note: We return 200 here because the GeoJSON update (the primary goal) succeeded.
        return (f"Analysis complete. GeoJSON saved. KML sync failed: {str(e)}", 200)
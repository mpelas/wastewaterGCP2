"""
Cloud Function to convert GeoJSON from GCS to KML and upload to Google Drive
This allows Google My Maps to import the data
"""

import json
import io
import functions_framework
from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from flask import jsonify
import simplekml

# Configuration
GCS_BUCKET_NAME = "mpelas-wastewater-bucket"
GEOJSON_PATH = "no_swim_zones/wastewater_no_swim_zones.geojson"
DRIVE_FOLDER_ID = "122jxF5nlwH8Re3ixoCjf2TuHyNCDuuxD"  # Set to your Google Drive folder ID, or None for root

# You'll need to set this up in GCP Secret Manager or as an environment variable
# For now, we'll use the default service account
SCOPES = ['https://www.googleapis.com/auth/drive.file']


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
        
        # Get properties for styling and info
        location = properties.get('location', 'Unknown Location')
        compliance = properties.get('Column1.compliance', None)
        details = properties.get('details', 'No details available')
        
        # Create description
        description = f"""
        <![CDATA[
        <b>Location:</b> {location}<br>
        <b>Compliance:</b> {'⚠️ NON-COMPLIANT' if compliance is False else '✓ Compliant'}<br>
        <b>Details:</b> {details}
        ]]>
        """
        
        # Determine color based on compliance
        if compliance is False:
            color = simplekml.Color.red
            style_url = '#non_compliant_style'
        else:
            color = simplekml.Color.blue
            style_url = '#compliant_style'
        
        # Add geometry to KML
        if geom_type == 'Polygon':
            coords = coordinates[0]  # Outer ring
            # Convert from [lon, lat] to [(lon, lat)]
            kml_coords = [(coord[0], coord[1]) for coord in coords]
            
            pol = kml.newpolygon(name=location, description=description)
            pol.outerboundaryis = kml_coords
            pol.style.polystyle.color = color
            pol.style.polystyle.fill = 1
            pol.style.polystyle.outline = 1
            pol.style.linestyle.color = simplekml.Color.white
            pol.style.linestyle.width = 2
            
        elif geom_type == 'MultiPolygon':
            for polygon in coordinates:
                coords = polygon[0]  # Outer ring of each polygon
                kml_coords = [(coord[0], coord[1]) for coord in coords]
                
                pol = kml.newpolygon(name=location, description=description)
                pol.outerboundaryis = kml_coords
                pol.style.polystyle.color = color
                pol.style.polystyle.fill = 1
                pol.style.polystyle.outline = 1
                pol.style.linestyle.color = simplekml.Color.white
                pol.style.linestyle.width = 2
        
        elif geom_type == 'Point':
            coords = coordinates
            pnt = kml.newpoint(name=location, description=description)
            pnt.coords = [(coords[0], coords[1])]
            pnt.style.iconstyle.color = color
    
    # Generate KML string
    kml_string = kml.kml()
    return kml_string


def upload_to_drive(file_content, filename, folder_id=None):
    """Upload file to Google Drive"""
    try:
        # Use default credentials (Cloud Function service account)
        # You may need to add Drive API permissions to this service account
        credentials = service_account.Credentials.from_service_account_info(
            info={},  # This will use default credentials
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
        
        # Make file accessible (optional)
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


@functions_framework.http
def sync_to_drive(request):
    """
    HTTP Cloud Function to sync GeoJSON from GCS to Google Drive as KML
    
    Usage:
    GET https://your-function-url/sync_to_drive
    POST https://your-function-url/sync_to_drive
    """
    
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)
    
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
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
        
        return jsonify({
            'success': True,
            'message': f'Successfully {drive_result["action"]} KML file in Google Drive',
            'feature_count': feature_count,
            'drive_file_id': drive_result['file_id'],
            'drive_link': drive_result['web_link'],
            'filename': filename
        }), 200, headers
        
    except Exception as e:
        print(f"ERROR: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500, headers
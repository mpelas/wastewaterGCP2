import json
import functions_framework
from shapely.geometry import Point, shape
from flask import jsonify

# Load GeoJSON data from the file
def load_geojson_data():
    with open("12kmBufferGreecePErifereiesWGS84.geojson", "r", encoding="utf-8") as file:
        geojson_data = json.load(file)
    return geojson_data

# Load GeoJSON data once (in memory)
geojson_data = load_geojson_data()

# Check if a point is inside any no-swim zone and return zone details
def check_no_swim_zone(latitude, longitude):
    point = Point(longitude, latitude)  # GeoJSON uses [longitude, latitude] order
    
    for feature in geojson_data["features"]:
        zone = shape(feature["geometry"])
        if point.within(zone):
            return True, feature
    
    return False, None

@functions_framework.http
def check_swim_zone(request):
    """HTTP Cloud Function to check if coordinates are in a no-swimming zone"""
    
    # Handle CORS
    if request.method == 'OPTIONS':
        headers = {
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Methods': 'GET, POST',
            'Access-Control-Allow-Headers': 'Content-Type',
            'Access-Control-Max-Age': '3600'
        }
        return ('', 204, headers)

    # Set CORS headers for actual request
    headers = {'Access-Control-Allow-Origin': '*'}
    
    try:
        # Get parameters from request
        if request.method == 'GET':
            latitude = request.args.get('latitude', type=float)
            longitude = request.args.get('longitude', type=float)
        elif request.method == 'POST':
            request_json = request.get_json()
            if request_json:
                latitude = request_json.get('latitude')
                longitude = request_json.get('longitude')
            else:
                return jsonify({'error': 'Invalid JSON'}), 400, headers
        else:
            return jsonify({'error': 'Method not allowed'}), 405, headers
            
        if latitude is None or longitude is None:
            return jsonify({'error': 'Missing latitude or longitude parameters'}), 400, headers
        
        # Check if point is in no-swim zone
        in_zone, zone_details = check_no_swim_zone(latitude, longitude)
        
        result = {
            'in_no_swim_zone': in_zone,
            'coordinates': {
                'latitude': latitude,
                'longitude': longitude
            }
        }
        
        # If in a no-swim zone, include full zone details
        if in_zone and zone_details:
            result['zone_details'] = zone_details['properties']
            result['zone_geometry'] = zone_details['geometry']
            
            # Highlight compliance status
            compliance = zone_details['properties'].get('Column1.compliance', None)
            if compliance is False:
                result['compliance_warning'] = "⚠️ NON-COMPLIANT ZONE - Column1.compliance: false"
                result['compliance_status'] = "NON_COMPLIANT"
            else:
                result['compliance_status'] = "COMPLIANT" if compliance else "UNKNOWN"
        
        return jsonify(result), 200, headers
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500, headers
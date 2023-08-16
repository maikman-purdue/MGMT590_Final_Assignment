# Parameters to pass:
''' {
"origin":"Indianapolis Aiport",
"destination": "Purdue university",
"project_id" : "mgmt590-343707",
"dataset_id" : "final_project",
"table_name" : "direction_api_data"
}'''


import requests
import os
from flask import Response
import traceback
from google.cloud import bigquery

def get_live_traffic_data(request):
    """Cloud function to get live traffic information using the Google Maps Platform Directions API and store it in BigQuery."""
    
    try:
        # Extract parameters from the request body
        request_data = request.get_json(silent=True)  # This will get the JSON body
        if not request_data:
            return Response("Request body is missing or not in JSON format.", status=400, mimetype='text/plain')

        origin = request_data.get('origin')
        destination = request_data.get('destination')
        project_id = request_data.get('project_id')
        dataset_id = request_data.get('dataset_id')
        table_name = request_data.get('table_name')
        api_key = os.environ.get('GOOGLE_MAPS_API_KEY')  # Get API key from environment variables

        # Construct the table_id using the provided parameters
        table_id = f"{project_id}.{dataset_id}.{table_name}"

        # Log the extracted parameters for debugging
        print(f"Origin: {origin}, Destination: {destination}, Table ID: {table_id}")

        # Check if all required parameters are provided
        if not all([origin, destination, project_id, dataset_id, table_name]):
            return Response("Parameters 'origin', 'destination', 'project_id', 'dataset_id', and 'table_name' are required.", status=400, mimetype='text/plain')

        # Create the API request.
        api_url = "https://maps.googleapis.com/maps/api/directions/json"
        params = {
            "origin": origin,
            "destination": destination,
            "traffic_model": "best_guess",
            "departure_time": "now",
            "alternatives": "true",
            "key": api_key
        }

        # Make the API request.
        response = requests.get(api_url, params=params)
        response_json = response.json()

        # Check if routes are available
        if not response_json.get("routes"):
            return Response("No available routes found.", status=400, mimetype='text/plain')

        # Extract data for BigQuery
        rows_to_insert = []

        for route in response_json["routes"]:
            base_data = {
                "start_address": route["legs"][0]["start_address"],
                "end_address": route["legs"][0]["end_address"],
                "start_lat": route["legs"][0]["start_location"]["lat"],
                "start_lng": route["legs"][0]["start_location"]["lng"],
                "end_lat": route["legs"][0]["end_location"]["lat"],
                "end_lng": route["legs"][0]["end_location"]["lng"],
                "summary": route.get("summary", "N/A")
            }

            for step in route["legs"][0]["steps"]:
                segment_data = {
                    "instruction": step.get("html_instructions", "N/A"),
                    "distance": step["distance"]["text"],
                    "duration": step["duration"]["text"],
                    "segment_start_lat": step["start_location"]["lat"],
                    "segment_start_lng": step["start_location"]["lng"],
                    "segment_end_lat": step["end_location"]["lat"],
                    "segment_end_lng": step["end_location"]["lng"],
                    "travel_mode": step["travel_mode"]
                }
                rows_to_insert.append({**base_data, **segment_data})

        # Insert data into BigQuery
        client = bigquery.Client()
        errors = client.insert_rows_json(table_id, rows_to_insert)
        if errors:
            return Response(f"Failed to insert rows into BigQuery: {errors}", status=500, mimetype='text/plain')

        return Response("Data successfully stored in BigQuery.", status=200, mimetype='text/plain')
    except Exception as e:
        error_message = f"Error: {e}\n{traceback.format_exc()}"
        return Response(error_message, status=500, mimetype='text/plain')

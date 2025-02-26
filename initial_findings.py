import requests
import json

# Open511 API endpoint
api_url = "https://api.open511.gov.bc.ca/events"

# Fetch real-time road incident data
response = requests.get(api_url)
if response.status_code == 200:
    data = response.json()
    print(json.dumps(data, indent=2))  # Pretty print JSON
else:
    print("Failed to fetch data:", response.status_code)
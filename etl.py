import json
import pandas as pd

# Define the file path
file_path = "/Users/ibrahimali/Desktop/CMPT733/CMPT733-ENV/Final_Project/incident.json"

# Load the JSON data
with open(file_path, "r", encoding="utf-8") as file:
    data = json.load(file)

# Extract the events data
events = data.get("events", [])

# Convert to DataFrame
df = pd.json_normalize(events)

# Change first 10 incidents' status to "INACTIVE"
df.loc[df.index[:10], "status"] = "INACTIVE"

# Ensure "created" and "updated" are parsed as proper datetime objects
df["created"] = pd.to_datetime(df["created"], errors="coerce", utc=True)
df["updated"] = pd.to_datetime(df["updated"], errors="coerce", utc=True)

# Extract only the date part
df["created"] = df["created"].dt.date
df["updated"] = df["updated"].dt.date

# Compute duration only for "INACTIVE" rows
df["duration"] = (pd.to_datetime(df["updated"]) - pd.to_datetime(df["created"])).where(df["status"] == "INACTIVE", pd.NaT)
print(df['duration'])
# Convert duration to an integer (days)
df["duration"] = df["duration"].dt.days

# Display the updated DataFrame
print(df)
schema = {col: str(df[col].dtype) for col in df.columns}
print("\nSchema of the DataFrame:")
print(json.dumps(schema, indent=4))
csv_file_path = "/Users/ibrahimali/Desktop/CMPT733/CMPT733-ENV/Final_Project/incidents_data.csv"
df.to_csv(csv_file_path, index=False)  # index=False prevents adding an extra index column

print(f"DataFrame successfully exported to: {csv_file_path}")
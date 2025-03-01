import pandas as pd
import ast
import matplotlib.pyplot as plt

# Pie chart of incident types
df = pd.read_csv('Data/incidents_data.csv')
df_group = df.groupby('event_type').size()

plt.figure(figsize=(4, 4))
plt.pie(df_group, labels=df_group.index, autopct='%1.1f%%', startangle=140)
plt.title('Incident Types')
plt.show()


# Scatter plot of events
df['geography.coordinates'] = df['geography.coordinates'].apply(lambda x: ast.literal_eval(x) if isinstance(x, str) else x)
coords = df['geography.coordinates'].apply(lambda x: x[0] if isinstance(x, list) and len(x) > 0 and isinstance(x[0], list) else x)
coords

longitudes = coords.str[0]
latitudes = coords.str[1]

plt.figure(figsize=(8, 6))
plt.scatter(longitudes, latitudes, alpha=0.7, edgecolors="k")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.title("Events Scatter Plot")
plt.show()
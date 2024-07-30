import json
from collections import Counter

# Load JSON data
file_path = r'snaptest\datasnap.json'
with open(file_path) as f:
    data = json.load(f)

# Extract stores data
stores_data = [record['stores'] for record in data]

# Count occurrences of each store
store_counter = Counter([store['store'] for store in stores_data])

# Identify duplicates
duplicates = {store: count for store, count in store_counter.items() if count > 1}

# Print duplicate stores and their counts
print("Duplicate store counts:")
for store, count in duplicates.items():
    print(f"{store}: {count}")

# Print details of the duplicate stores
print("\nDetails of duplicate stores:")
for store in duplicates:
    print(f"\nStore ID: {store}")
    for record in stores_data:
        if record['store'] == store:
            print(record)

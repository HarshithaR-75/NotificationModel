import firebase_admin
from firebase_admin import credentials
from firebase_admin import db
import pandas as pd

# Initialize the Firebase app with the service account key
cred = credentials.Certificate('notificationapplication-9f0b8-firebase-adminsdk-t02hd-363c9b0a93.json')
firebase_admin.initialize_app(cred, {
    'databaseURL': 'https://notificationapplication-9f0b8-default-rtdb.firebaseio.com/'
})

# Reference the database location
ref = db.reference('compressed_notifications')

# Fetch the data
data = ref.get()

# Print the raw data to debug
print("Raw Data from Firebase:", data)

# Function to flatten hierarchical data
def flatten_data(data):
    flattened_data = []

    for root_id, platforms in data.items():
        print(f"Root ID: {root_id}")  # Debugging print
        if not isinstance(platforms, dict):
            print(f"Platforms for root ID {root_id} are not a dictionary. Skipping.")
            continue
        
        for platform, categories in platforms.items():
            print(f"  Platform: {platform}")  # Debugging print
            if not isinstance(categories, dict):
                print(f"  Categories for platform {platform} are not a dictionary. Skipping.")
                continue
            
            for category, details in categories.items():
                print(f"    Category: {category}")  # Debugging print
                if not isinstance(details, dict):
                    print(f"    Details for category {category} are not a dictionary. Skipping.")
                    continue
                
                print(f"    Details: {details}")  # Debugging print
                # Extract only channel_id and importance
                flattened_record = {'channel_id': category, 'device_id': root_id, 'package': platform}
                
                if 'importance' in details:
                    flattened_record['importance'] = details['importance']
                    flattened_data.append(flattened_record)
                else:
                    print(f"    'importance' key not found in details. Skipping.")

    return flattened_data

# Flatten the data
flattened_data = flatten_data(data)

# Convert to DataFrame
df = pd.DataFrame(flattened_data)

# Print the DataFrame to debug
print("Flattened DataFrame:")
print(df)

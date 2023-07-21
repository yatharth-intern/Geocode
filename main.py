from geopy.geocoders import Nominatim
import pandas as pd
from geopy.distance import geodesic
import time
import math
from concurrent.futures import ThreadPoolExecutor

geolocator = Nominatim(user_agent='geocode')

# Reading the CSV file
pincode_file = pd.read_csv('user_pincode_address.csv', delimiter=',')
lat_long_file = pd.read_csv('user_lat_long.csv', delimiter=',')

# Create an empty DataFrame to store user distances
df_user_distances = pd.DataFrame(columns=['User', 'Distance'])

user_list = pincode_file['user'].tolist()

# Add a delay before the loop
time.sleep(1)

# Counter to keep track of iterations
iteration_counter = 0

# Dictionary to cache the location for each zipcode_phrase
location_cache = {}

start_user_id = 1863011

# Check if the start_user_id exists in user_list
if start_user_id in user_list:
    # Find the index of the starting user ID in user_list
    start_index = user_list.index(start_user_id)
else:
    print(f"Start user ID {start_user_id} not found in the user list.")
    start_index = -1

# Proceed with the loop only if the start_user_id is found
if start_index != -1:
    batch_size = 100  # Define the batch size for processing

    # Function to perform geocoding with retries
    def geocode_with_retries(zipcode_phrase, max_retries):
        retries = 0
        while retries < max_retries:
            try:
                location = geolocator.geocode(zipcode_phrase, timeout=10)
                return location
            except Exception as e:
                retries += 1
                print(f"Geocoding failed. Retrying... ({retries}/{max_retries})")
                time.sleep(1)
        return None

    # Use ThreadPoolExecutor for parallel processing
    with ThreadPoolExecutor(max_workers=5) as executor:
        # Run the loop in batches
        for batch_start in range(start_index, len(user_list), batch_size):
            batch_end = min(batch_start + batch_size, len(user_list))
            batch_users = user_list[batch_start:batch_end]

            # Create temporary DataFrames to hold batch data
            batch_pincode_data = pincode_file[pincode_file['user'].isin(batch_users)]
            batch_lat_long_data = lat_long_file[lat_long_file['user'].isin(batch_users)]

            # Merge the temporary DataFrames to get the complete data for the batch
            batch_data = pd.merge(batch_pincode_data, batch_lat_long_data, on='user')

            # Filter out rows with missing data
            batch_data = batch_data.dropna(subset=['pincode', 'lat', 'long'])

            if not batch_data.empty:
                # Function to process a single row asynchronously
                def process_row(row):
                    # Extract data from the row
                    global df_user_distances
                    user_id = int(row['user'])
                    zipcode_phrase = str(int(row['pincode'])) + " India"
                    exist_lat = row['lat']
                    exist_long = row['long']

                    # Check for invalid or missing data
                    if math.isnan(exist_lat) or math.isnan(exist_long):
                        print(f"Skipping User: {user_id} - Missing latitude or longitude data")
                        return None

                    try:
                        # Check if location is already cached
                        if zipcode_phrase in location_cache:
                            location = location_cache[zipcode_phrase]
                        else:
                            # Geocode with retries
                            location = geocode_with_retries(zipcode_phrase, max_retries=3)
                            # Cache the location for future use
                            location_cache[zipcode_phrase] = location

                        if location:
                            new_lat = location.latitude
                            new_long = location.longitude
                            location1 = (exist_lat, exist_long)
                            location2 = (new_lat, new_long)

                            # Calculate the distance in km
                            dist = geodesic(location2, location1).km
                            print(f"User: {user_id}, Distance: {dist} km")

                            # Create a temporary DataFrame for the current user distance
                            df_temp = pd.DataFrame({'User': [user_id], 'Distance': [dist]})

                            # Concatenate the temporary DataFrame with the main DataFrame
                            df_user_distances = pd.concat([df_user_distances, df_temp], ignore_index=True)

                    except Exception as e:
                        print(f"Error processing User: {user_id} - {str(e)}")
                        return None

                # Process users asynchronously
                results = list(executor.map(process_row, batch_data.to_dict(orient='records')))

                # Save the DataFrame to the CSV file after each batch iteration
                df_user_distances.to_csv('user_distances.csv', index=False)

                # Increment the iteration_counter
                iteration_counter += 1

                # Add a delay after each batch request
                time.sleep(1)

    # Save the remaining user distances to the CSV file
    df_user_distances.to_csv('user_distances.csv', index=False)

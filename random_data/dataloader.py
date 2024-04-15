import os
import pandas as pd
from pymongo import MongoClient

# MongoDB Atlas connection URI
mongo_uri = "mongodb+srv://avikpat:12345@cluster0.kus5z.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

# Name of the database
db_name = "Jman_new"

# Dictionary to map CSV file names to collection names
collection_mapping = {
    "combinedModels.csv": "combinedModels",
    "Modules.csv": "Modules",
    "Performance.csv": "Performance",
    "users.csv": "users"
}

# Directory containing CSV files
csv_files_directory = "./"

def load_data_to_mongodb(csv_files_directory, uri, db_name, collection_mapping):
    # Connect to MongoDB
    client = MongoClient(uri)
    
    # Access the specified database
    db = client[db_name]
    
    # Iterate over the CSV file names and their corresponding collection names
    for csv_file, collection_name in collection_mapping.items():
        # Path to the CSV file
        csv_file_path = os.path.join(csv_files_directory, csv_file)
        
        # Load CSV file into a pandas DataFrame
        df = pd.read_csv(csv_file_path)
        
        # Convert DataFrame to dictionary for easier insertion into MongoDB
        data = df.to_dict(orient='records')
        
        # Access the collection or create it if it doesn't exist
        collection = db[collection_name]
        
        # Insert data into the collection
        collection.insert_many(data)
        
        print(f"Data from {csv_file} successfully loaded into {collection_name} collection.")

# Call the function to load data into MongoDB
load_data_to_mongodb(csv_files_directory, mongo_uri, db_name, collection_mapping)

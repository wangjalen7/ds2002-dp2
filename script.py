import os
import json
from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo.errors import ConnectionFailure

def main():
    # Set up MongoDB connection to personal MongoDB Atlas account
    MONGOPASS = os.getenv('MONGOPASS')
    uri = "mongodb+srv://cluster0.q0nrosd.mongodb.net/"
    client = MongoClient(uri, username='zac9nk', password=MONGOPASS, connectTimeoutMS=200, retryWrites=True)
    db = client.Data_Project_2
    collection = db.JSON_Files

    directory = 'data/'

    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            filepath = os.path.join(directory, filename)
            try:
                with open(filepath, 'r') as file:
                    data = json.load(file)

                if isinstance(data, list):
                    collection.insert_many(data)
                else:
                    collection.insert_one(data)
                print(f"Successfully imported {filename}")
            except json.JSONDecodeError as e:
                print(f"Failed to decode JSON from {filename}: {e}")
            except FileNotFoundError as e:
                print(f"File not found {filename}: {e}")
            except BulkWriteError as e:
                print(f"MongoDB bulk write error with {filename}: {e}")
            except Exception as e:
                print(f"An error occurred with {filename}: {e}")
            except ConnectionFailure as e:
                print(f"MongoDB connection error with {filename}: {e}")

if __name__ == '__main__':
    main()

from pymongo import MongoClient
import csv

# specify the MongoDB server host and port
host = "localhost"
port = 27017

# create a MongoClient instance
client = MongoClient(host, port)

# specify the database and collection names
db_name = "CityBike"
collection_name = "CityBikeRanking"

# get a reference to the database and collection
db = client[db_name]
collection = db[collection_name]

# open the CSV file and read the data
with open("abgehendefahrten.csv/part-00000-4b3640b6-014f-44f3-9e39-1e0a581ab667-c000.csv") as csv_file:
    reader = csv.DictReader(csv_file)
    data = list(reader)

# insert the data into the collection
result = collection.insert_many(data)

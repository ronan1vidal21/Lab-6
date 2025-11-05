from pymongo import MongoClient
import pprint
import re

# -----------------------------------------------
# Connect to MongoDB
# -----------------------------------------------
client = MongoClient("mongodb://localhost:27017/")
db = client["LAB6"]
collection = db["MyCollection"]

# -----------------------------------------------
# 1. SORT CUSTOMERS BY LAST AND FIRST NAME
# -----------------------------------------------
print("Sorted output of customers by LastName and FirstName:\n")

customers = []

# Go through each document in MyCollection
for doc in collection.find({"Customer": {"$exists": True}}):
    customer_list = doc["Customer"]
    # Extend the list if there are multiple customers
    if isinstance(customer_list, list):
        customers.extend(customer_list)
    else:
        customers.append(customer_list)

# Sort alphabetically by LastName, then FirstName
customers = sorted(customers, key=lambda c: (c["LastName"], c["FirstName"]))

# Display sorted customers
for c in customers:
    pprint.pprint(c)

# -----------------------------------------------
# 2. FILTER CUSTOMERS WHOSE LASTNAME STARTS WITH 'G'
# -----------------------------------------------
print("\nCustomers whose LastName starts with 'G':\n")

regex = re.compile('^G.*$', re.IGNORECASE)
filtered_customers = [c for c in customers if regex.match(c["LastName"])]

for c in filtered_customers:
    pprint.pprint(c)

print("\n# of documents found:", len(filtered_customers))


from pymongo import MongoClient

# Connect to MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["LAB6"]

# Create and populate the genres collection
genres_collection = db["genres"]

genres_data = [
    {"GenreId": 1, "Name": "Rock"},
    {"GenreId": 2, "Name": "Jazz"},
    {"GenreId": 3, "Name": "Metal"},
    {"GenreId": 4, "Name": "Alternative & Punk"},
    {"GenreId": 5, "Name": "Pop"},
    {"GenreId": 6, "Name": "Latin"},
    {"GenreId": 7, "Name": "Blues"},
    {"GenreId": 8, "Name": "Classical"},
    {"GenreId": 9, "Name": "Reggae"},
    {"GenreId": 10, "Name": "Soundtrack"}
]

genres_collection.insert_many(genres_data)
print("Genres collection created successfully!")

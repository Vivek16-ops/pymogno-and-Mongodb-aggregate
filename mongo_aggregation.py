# Aggregation pipeline in MongoDb
import json
import pymongo

# Establishing Connection
client = pymongo.MongoClient('mongodb://localhost:27017/')

# Creating a database for house pricing
db = client['House-pricing-aggregation']

# creating a collection
collection = db.Houses

# Inseritng entries into database
priceinfo = [
    {"location": "Kolkata", "price": 3, "rooms": 3, "floor": 3},
    {"location": "Kolkata", "price": 3, "rooms": 2, "floor": 20},
    {"location": "Ranchi", "price": 3, "rooms": 3, "floor": 5},
    {"location": "Dhanbad", "price": 2, "rooms": 6, "floor": 2},
    {"location": "Delhi", "price": 5, "rooms": 4, "floor": 22},
    {"location": "Chennai", "price": 4, "rooms": 3, "floor": 40},
    {"location": "Patna", "price": 2, "rooms": 500000, "floor": 45},
    {"location": "Aurangabad", "price": 50000, "rooms": 1, "floor": 7},
    {"location": "Assam", "price": 1, "rooms": 4, "floor": 12},
    {"location": "Tripura", "price": 3, "rooms": 2, "floor": 9},
    {"location": "Kolkata", "price": 3, "rooms": 1, "floor": 67},
    {"location": "Kolkata", "price": 3, "rooms": 4, "floor": 0},
    {"location": "Kolkata", "price": 3, "rooms": 2, "floor": 23},
    {"location": "Kolkata", "price": 3, "rooms": 4, "floor": 1},
    {"location": "Kolkata", "price": 3, "rooms": 1, "floor": 0},
    {"location": "Kolkata", "price": 3, "rooms": 2, "floor": 2}
]
# housing = collection.insert_many(priceinfo)
# Operations in a typical aggregation pipeline
# input ---> $match---> $group---> $start---> output

documents = db.Houses.aggregate([
    {"$match": {"location": "Kolkata"}},
    {"$group": {"_id": "$_id","price":{"$first":"$price"},"floorooms":{"$sum":{"$add":["$floor","$rooms"]}}}},
    {"$sort": {"floorooms": 1,"price":-1}},
])
for document in documents:
    print(document)

o=[]

with open("data.json","a")as f:
    for document in documents:
       o.append({"location":str(document["_id"]),"price":document["price"],"floorooms":document["floorooms"]})

    f.write(json.dumps(o))
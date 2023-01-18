# This code is written with the help of pymongo documentation
from typing import Collection
import pymongo

# function to insert document
def insertdocument():
    studentinfo = {
        "name": "Harsh",
        "section": "E",
        "maths_marks": 90,
        "sst_marks": 100,
        "c++":89,
        "Accounts":99
    }
    student_id = collection.insert_one(studentinfo).inserted_id
    print(student_id, "has been inserted")


# Function for read document
def read_document():
    for student in mystudent:
        print(student)


if __name__ == '__main__':

    # for establishing connections
    client = pymongo.MongoClient('mongodb://localhost:27017/')

    # creating a database
    db = client['pymongo-database']

    # creating collection and add values
    # posts = db.posts
    # post_id = posts.insert_one({"p": 1}).inserted_id
    # post_id = posts.insert_one({"p2": 2}).inserted_id
    # print(post_id)

    # //creating another collection
    collection = db.class1

# CRUD: Create , Read , Update ,Delete
    # 1) Create
    # Inserting a document
    # insertdocument()

    # 2)Read
    mystudent = collection.find({})
    print(mystudent)
    read_document()

    # 3)Update 
    # collection.update_one({"section":"A"},{'$inc':{'sst_marks':1}})
    # read_document()

    # 4) Delete 
    # collection.delete_one({"section":"Z"})this does not make any changes becoz this section is not exist

print("Abhi tak to thik hai")

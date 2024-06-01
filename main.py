from dotenv import load_dotenv,find_dotenv
from pymongo import MongoClient
import os
import pprint

load_dotenv(find_dotenv())

password = os.environ.get("MONGODB_PWD")
connection_string =f"mongodb+srv://bharathh30:{password}@cluster0.2v8j4dk.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

client = MongoClient(connection_string)

# To Print the databases we use the below code
dbs = client.list_database_names()
# print(dbs)

# In order to access the database 
test_db = client.test
collections = test_db.list_collection_names()
# print(collections)

# Databases classified into collections classified into forms or data (BSON is used and NoSQL so the data is stored in the key value pair)

# function to insert document 
def insert_test_doc():
    collection = test_db.test #initialising the collection name 
    test_document = {
        "name" : "Bujji",
        "type" : "Test"
    }
    inserted_id = collection.insert_one(test_document).inserted_id
    print(inserted_id)

# insert_test_doc()

# Practicing

production = client.production
person_collection = production.person_collection

def create_documents():
    first_names = ["Tim" , "Sarah" , "Jennifer" , "Jose" , "Brad" , "Allen"]
    last_names = ["Ruscica" , "Smith" , "Bart" , "Carter" , "Pit" , "Geral"]
    ages = [21,40,23,19,34,67]

    # one way of inserting documents is 

    # for first_name,last_name,age in zip(first_names,last_names,ages):
    #     doc = {"first_name" : first_name , "last_name" : last_name , "age" : age}
    #     person_collection.insert_one(doc)

    # The effiecient way is using a list 
    docs = []
    for first_name,last_name,age in zip(first_names,last_names,ages):
        doc = {"first_name" : first_name , "last_name" : last_name , "age" : age}
        docs.append(doc)

    person_collection.insert_many(docs)

# create_documents()

printer = pprint.PrettyPrinter()

def find_all_people():
    people = person_collection.find()

    for person in people:
        printer.pprint(person)

# find_all_people()

# Searching for a specific valuee

def find_tim():
    tim = person_collection.find_one({"first_name":"Tim"})
    printer.pprint(tim)

# find_tim()

def count_all_people():
    # count = person_collection.count_documents(filter={})
    # count = person_collection.find().count()  
    count = person_collection.count_documents(filter={"first_name":"Tim"})
    print("Number of People",count)

# count_all_people()

def get_person_by_id(person_id :str):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)
    person = person_collection.find_one({"_id":_id})
    printer.pprint(person)

# get_person_by_id("665b130bdf7fe275e6f79564")

def get_age_range(min_age,max_age):
    query = {
        "$or":[
            {"age" : {"$gte" : min_age}},
            {"age" : {"$lte" : max_age}}
        ]
    }
    people = person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

# get_age_range(20,35)

def project_columns():
    columns1={"_id":0,"first_name":1,"last_name":1} # 0  if we dont want to display that values , 1 if we want to display those values
    people = person_collection.find({},columns1)
    for person in people:
        printer.pprint(person)

# project_columns()

# Updating

def update_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id = ObjectId(person_id)
    # write a query for updating the database valuess

    all_updates = {
        "$set" : {"new_field" : True},  #this will set the values of new_field to True , similar to overriding
        "$inc" : {"age" : 1},  #it will increment the age by 1
        "$rename" : {"first_name":"first" , "last_name":"last"} #rename is used to rename the names of the fields (first_name,last_name,age)
    }
    # # collection name . update_one ({field:field} , query we have writter (things we wanted to apply))
    # person_collection.update_one({"_id":_id},all_updates)

    # writing a query for unsetting or removing the data
    # Method 1
    # unsetting = {
    #     "$unset" : {"new_field":""}
    # }
    # person_collection.update_one({"_id":_id},unsetting)
    
    # Method 2
    person_collection.update_one({"_id":_id},{"$unset" : {"new_field" : ""}})

# update_person_by_id("665b130bdf7fe275e6f79562")

# replacing current information with new information

def replace_one(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    new_doc = {
        "first_name" : "new first name", #Tim becomes new first name
        "last_name" : "new last name", #Ruscica becomes new last name
        "age" : 100 #100 becomes to new age 
    } 

    person_collection.replace_one({"_id":_id},new_doc)

# replace_one("665b130bdf7fe275e6f79562")

# Delete operation

def delete_by_id(person_id):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.delete_one({"_id":_id})
    # person_collection.delete_one({})  -- For deleting every document in the collection

delete_by_id("665b130bdf7fe275e6f79562")

# def delete_by_providing_field():
#     person_collection.delete_one({"first_name" : "Sarah"})

# delete_by_providing_field()

# ------------------------------------------------------------

# Relationships

address ={
    "_id" : "665b130bdf7fe275e6f79562",
    "street" : "Bay Street",
    "number" : 2706,
    "city" : "San Fransisco",
    "country" : "United States",
    "zip" : "94107"
}

# One of the method for relationships when the data is not that  big

person = {
    "_id" : "665b130bdf7fe275e6f79512",
    "first_name" : "John",

        "address" : {
        "_id" : "665b130bdf7fe275e6f79562",
        "street" : "Bay Street",
        "number" : 2706,
        "city" : "San Fransisco",
        "country" : "United States",
        "zip" : "94107"
    }
}

# other method is using foreign key

address1 ={
    "_id" : "665b130bdf7fe275e6f79562",
    "street" : "Bay Street",
    "number" : 2706,
    "city" : "San Fransisco",
    "country" : "United States",
    "zip" : "94107",
    "owner_id" : "665b130bdf7fe275e6f79512"  # we specifed the john id and created a relationship between address and person
}

person = {
    "_id" : "665b130bdf7fe275e6f79512",
    "first_name" : "John"
}


# Application of this relation methods

# Method 1
def add_address_embed(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    person_collection.update_one({"_id":_id},{"$addToSet":{"addresses":address}}) #an array of data will be added 

# add_address_embed("665b130bdf7fe275e6f79566",address)

# Method 2
def add_address_relationship(person_id,address):
    from bson.objectid import ObjectId
    _id = ObjectId(person_id)

    address = address.copy()
    address["owner_id"] = person_id

    address_collection = production.address
    address_collection.insert_one(address)

add_address_relationship("665b130bdf7fe275e6f79564",address)
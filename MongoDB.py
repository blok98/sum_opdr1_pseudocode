import psycopg2
import random
from pymongo import MongoClient

def get_database(coll):
    client = MongoClient('mongodb://localhost:27017')
    db = client['dataset_hu']
    collection_student = db[coll]
    data = collection_student.find().limit(200)
    return data

def get_data(coll):
    data=get_database(coll)
    data_list=[]
    for datapoint in data:
        data_list.append(datapoint)
    return data_list
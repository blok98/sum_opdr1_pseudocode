import psycopg2
import random
from pymongo import MongoClient

def get_database(coll,limiet):
    client = MongoClient('mongodb://localhost:27017')
    db = client['dataset_hu']
    collection_student = db[coll]
    data = collection_student.find().limit(limiet)
    return data

def get_data(coll,limiet):
    data=get_database(coll,limiet)
    data_list=[]
    for datapoint in data:
        data_list.append(datapoint)
    return data_list
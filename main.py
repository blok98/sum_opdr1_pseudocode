from pymongo import MongoClient
import functions.py

client             = MongoClient('mongodb://localhost:27017')
db                 = client['dataset_hu']
collection_student = db['products']
data               = collection_student.find()

data_list=functions.data_list()
cursor=functions.connect('dataset_hu','toegang','Tom-1998','127.0.0.1')

qry_columns = '_id varchar(255),brand varchar(255),category varchar(255),color varchar(255),flavor varchar(255),gender varchar(255),herhaalaankopen varchar(255),name varchar(255),predict_out_of_stock_date varchar(255),price varchar(255),recommendable varchar(255),stock varchar(255),sub_category varchar(255),sub_sub_category varchar(255)'
functions.create_table_products(qry_columns)

functions.insertion(cursor,data)

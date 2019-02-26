from pymongo import MongoClient
from pack import functions
import pack
from pack import functions



data_list= functions.get_data_products()
conn= functions.connect('postgres', 'toegang', 'Tom-1998', '127.0.0.1')
cursor=conn.cursor()

qry_columns = '_id varchar(255),brand varchar(255),category varchar(255),color varchar(255),flavor varchar(255),gender varchar(255),herhaalaankopen varchar(255),name varchar(255),predict_out_of_stock_date varchar(255),price varchar(255),recommendable varchar(255),stock varchar(255),sub_category varchar(255),sub_sub_category varchar(255)'
functions.create_table(conn,'products',qry_columns)

functions.insertion(conn,data_list)


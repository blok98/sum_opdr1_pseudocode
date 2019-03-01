from pymongo import MongoClient
from pack import functions
import pack
from pack import functions



data_list_products= functions.get_data('products')
data_list_sessions= functions.get_data('sessions')
conn= functions.connect('postgres', 'toegang', 'Tom-1998', '127.0.0.1')
cursor=conn.cursor()

functions.drop_table(conn,'stock')
functions.drop_table(conn,'sessions')
functions.drop_table(conn,'products')

qry_columns = '_id varchar(255),brand varchar(255),category varchar(255),color varchar(255),flavor varchar(255),gender varchar(255),herhaalaankopen varchar(255),name varchar(255),predict_out_of_stock_date varchar(255),price varchar(255),recommendable varchar(255),sub_category varchar(255),sub_sub_category varchar(255), PRIMARY KEY(_id)'
functions.create_table(conn,'products',qry_columns)

qry_columns = '_id varchar(255) REFERENCES products(_id), date varchar(255), stock_level varchar(255), PRIMARY KEY(_id,date)'
functions.create_table(conn,'stock',qry_columns)

qry_columns = '_id varchar(255) PRIMARY KEY,session_start varchar(255), session_end varchar(255), has_sale varchar(255), cg_created varchar(255), tg_created varchar(255)'
functions.create_table(conn,'sessions',qry_columns)

functions.insertion_products(conn,data_list_products)
functions.insertion_sessions(conn,data_list_sessions)


import MongoDB
import SQL
import Manage_Tables
import Insertion

data_list_products= MongoDB.get_data('products')
data_list_sessions= MongoDB.get_data('sessions')
data_list_visitors= MongoDB.get_data('visitors')

conn= SQL.connect('postgres', 'toegang', 'Tom-1998', '127.0.0.1')
cursor=conn.cursor()

Manage_Tables.drop_table(conn,'recommendation')
Manage_Tables.drop_table(conn,'order_products')
Manage_Tables.drop_table(conn,'stock')
Manage_Tables.drop_table(conn,'products')
Manage_Tables.drop_table(conn,'sessions')
Manage_Tables.drop_table(conn,'visitors')



qry_columns = '_id varchar(255),brand varchar(255),category varchar(255),color varchar(255),flavor varchar(255),gender varchar(255),herhaalaankopen varchar(255),name varchar(255),predict_out_of_stock_date varchar(255),price varchar(255),recommendable varchar(255),sub_category varchar(255),sub_sub_category varchar(255), PRIMARY KEY(_id)'
Manage_Tables.create_table(conn,'products',qry_columns)

qry_columns = '_id varchar(255) REFERENCES products(_id), date varchar(255), stock_level varchar(255), PRIMARY KEY(_id,date)'
Manage_Tables.create_table(conn,'stock',qry_columns)

qry_columns = '_id varchar(255) PRIMARY KEY, latest_activity varchar(255), latest_visit varchar(255), buids varchar(5000) UNIQUE'
Manage_Tables.create_table(conn,'visitors',qry_columns)

qry_columns = '_id varchar(255) PRIMARY KEY,visitor_buids varchar(5000) REFERENCES visitors(buids),session_start varchar(255), session_end varchar(255), has_sale varchar(255), cg_created varchar(255), tg_created varchar(255)'
Manage_Tables.create_table(conn,'sessions',qry_columns)
#in visitors staan meerdere buids, per session is er 1 buid. 1 visitor heeft dus meerdere sessies maar dit is de enige link tussen de 2. Dus hoe foreign key?

qry_columns = 'products_id varchar(255) REFERENCES products(_id), sessions_id varchar(255) REFERENCES sessions(_id), PRIMARY KEY(products_id,sessions_id)'
Manage_Tables.create_table(conn,'order_products',qry_columns)

qry_columns='visitors_id varchar(255) REFERENCES visitors(_id),products_id varchar(255) REFERENCES products(_id),timestamp varchar(255),segment varchar(255),latest_visit varchar(255),total_pageview_count varchar(255),total_view_count varchar(255),PRIMARY KEY(visitors_id,products_id)'
Manage_Tables.create_table(conn,'recommendation',qry_columns)

Insertion.insertion_products(conn, data_list_products)
#visitors_id_list=Insertion.insertion_visitors(conn, data_list_visitors)
#functions.insertion_sessions(conn,data_list_sessions,visitors_id_list)

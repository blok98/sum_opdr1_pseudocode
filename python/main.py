import MongoDB
import SQL
import Manage_Tables
import Insertion

limiet=10

data_list_products= MongoDB.get_data('products',limiet)
data_list_sessions= MongoDB.get_data('sessions',limiet)
data_list_visitors= MongoDB.get_data('visitors',limiet)

conn              = SQL.connect('postgres', 'toegang', 'Tom-1998', '127.0.0.1')
cursor            = conn.cursor()


# cursor.execute("select * from information_schema.tables where table_name=%s", ('products',))
# exist=bool(cursor.rowcount)
# print('products',exist)

Manage_Tables.drop_table(conn,'similar_product')
Manage_Tables.drop_table(conn,'recommendation')
Manage_Tables.drop_table(conn,'order_products')
Manage_Tables.drop_table(conn,'stock')
Manage_Tables.drop_table(conn,'products')
Manage_Tables.drop_table(conn,'sessions')
Manage_Tables.drop_table(conn,'visitors_buids')
Manage_Tables.drop_table(conn,'visitors')
print('\n')

products_attributes={'_id':'varchar(255)','brand':'varchar(255)','category':'varchar(255)','gender':'varchar(255)','herhaalaankopen':'varchar(255)','predict_out_of_stock_date':'varchar(255)','price':'varchar(255)','recommendable':'varchar(255)','sub_category':'varchar(255)','sub_sub_category':'varchar(255)'}
stock_attributes={'_id':'varchar(255)','date':'varchar(255)','stock_level':'varchar(255)'}
order_products_attributes={'product_id':'varchar(255)','session_id':'varchar(255)'}
visitors_attributes={'_id':'varchar(255)','latest_activity':'varchar(255)','latest_visit':'varchar(255)'}
visitors_buids_attributes={'_id':'varchar(255)','buid':'varchar(255)'}
sessions_attributes={'_id':'varchar(255)','session_start':'varchar(255)','session_end':'varchar(255)','has_sale':'varchar(255)','buid':'varchar(255)'}
recommendation_attributes={'_id':'varchar(255)','timestamp':'varchar(255)','segment':'varchar(255)','latest_visit':'varchar(255)'}
similar_product_attributes={'v_id':'varchar(255)','p_id':'varchar(255)'}


Manage_Tables.create_table(conn,'products',products_attributes,primary_key='_id', foreign_key=False)
Manage_Tables.create_table(conn,'stock',stock_attributes,primary_key='_id,date', foreign_key={'_id':'products(_id)'})
Manage_Tables.create_table(conn,'sessions',sessions_attributes,primary_key='_id', foreign_key=False)
Manage_Tables.create_table(conn,'visitors',visitors_attributes,primary_key='_id', foreign_key=False)
Manage_Tables.create_table(conn,'visitors_buids',visitors_buids_attributes,primary_key='_id, buid', foreign_key={'_id':'visitors(_id)'})
Manage_Tables.create_table(conn,'recommendation',recommendation_attributes,primary_key='_id', foreign_key={'_id':'visitors(_id)'})
Manage_Tables.create_table(conn,'similar_product',similar_product_attributes,primary_key='v_id,p_id', foreign_key={'v_id':'recommendation(_id)'})
Manage_Tables.create_table(conn,'order_products',order_products_attributes,primary_key='product_id,session_id', foreign_key={'product_id':'products(_id)','session_id':'sessions(_id)'})
print('\n')

# Insertion.insertion_products(conn, data_list_products)
# Insertion.insertion_visitors(conn, data_list_visitors)
# Insertion.insertion_sessions(conn, data_list_sessions)


Insertion.newInsertion_products(conn,coll1='products',coll2='stock',data_input=data_list_products,attributes=products_attributes,attributes2=stock_attributes,fk='_id')
Insertion.newInsertion_visitors(conn,coll1='visitors',coll2='visitors_buids',coll3='recommendation',coll4='similar_product',data_input=data_list_visitors,attributes=visitors_attributes,attributes2=visitors_buids_attributes,attributes3=recommendation_attributes,attributes4=similar_product_attributes,fk='_id')
Insertion.newInsertion_sessions(conn,coll1='sessions',coll2='order_products',data_input=data_list_sessions,attributes=sessions_attributes,attributes2=order_products_attributes,fk='_id')




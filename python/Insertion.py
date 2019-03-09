import SQL
import Modify


def newInsertion_products(conn,coll1,coll2,data_input,attributes,attributes2,fk):
    count=0
    for datapoint in data_input:
        query = "INSERT INTO {} VALUES(".format(coll1)
        count+=1
        for element in attributes:
            if element in datapoint:
                if element == 'price':
                    attribute = datapoint[element]['selling_price']
                else:
                    attribute = datapoint[element]
            else:
                attribute = 'Empty'
            if element == fk:
                data_fk = attribute

            query = Modify.change_query(query, attribute)

        query = query[:-1] + ')'
        SQL.sqlexecute(conn, query,on_conflict=False)

        insertion_stock(conn, coll2, datapoint, attributes2,fk, data_fk)


def insertion_stock(conn,coll,datapoint,attributes,fk,data_fk):
    data=datapoint['stock']
    for element in data:
        query = "INSERT INTO {} VALUES(".format(coll)
        for att in attributes:
            if att == fk:
                attribute=data_fk
                attribute = str(attribute).replace("'", "")
                query += "'" + attribute + "'" + ","
            elif att in element:
                attribute=element[att]
                attribute=str(attribute).replace("'","")
                query += "'" + attribute + "'" + ","
            else:
                attribute='Empty'
                query += "'" + attribute + "'" + ","
        query = query[:-1] + ')'
        SQL.sqlexecute(conn, query, on_conflict=False)

def newInsertion_visitors(conn,coll1,coll2,coll3,coll4,data_input,attributes,attributes2,attributes3,attributes4,fk):
    count=0
    for datapoint in data_input:
        query = "INSERT INTO {} VALUES(".format(coll1)
        count+=1
        for element in attributes:
            if element in datapoint:
                attribute = datapoint[element]
            else:
                attribute = 'Empty'
            if element == fk:
                data_fk = attribute
            query = Modify.change_query(query, attribute)
        query = query[:-1] + ')'
        SQL.sqlexecute(conn, query, on_conflict=False)

        insertion_visitors_buids(conn, coll2, datapoint, attributes2,fk,data_fk)
        insertion_recommendation(conn,coll3,datapoint,attributes3,attributes4,fk,data_fk)

def insertion_visitors_buids(conn,coll,datapoint,attributes,fk,data_fk):
    if 'buids' in datapoint:
        data=datapoint['buids']
        for element in data:
            query = "INSERT INTO {} VALUES('{}',".format(coll,data_fk)
            attribute = str(element).replace("'", "")
            query += "'" + attribute + "'" + ","
            query = query[:-1] + ')'
            SQL.sqlexecute(conn, query, on_conflict=False)

def insertion_recommendation(conn,coll,datapoint,attributes,attributes2,fk,data_fk):
    query = "INSERT INTO {} VALUES(".format(coll)
    if 'recommendations' in datapoint:
        recommendations=datapoint['recommendations']
        for element in attributes:
            if element==fk:
                attribute=data_fk
            elif element in recommendations:
                attribute=recommendations[element]
            else:
                attribute='Empty'
            query=Modify.change_query(query,attribute)

        query = query[:-1] + ')'
        SQL.sqlexecute(conn, query, on_conflict=False)
        insertion_similar_product(conn,'similar_product',recommendations,attributes2,fk,data_fk)

def insertion_similar_product(conn,coll,datapoint,attributes,fk,data_fk):
    similars=datapoint['similars']
    for similar in similars:
        query = "INSERT INTO {} VALUES('{}',".format(coll, data_fk)
        query=Modify.change_query(query,similar)
        query = query[:-1] +')'
        SQL.sqlexecute(conn,query,on_conflict=False)


def newInsertion_sessions(conn,coll1,coll2,data_input,attributes,attributes2,fk):
    ignored_records = 0
    for datapoint in data_input:
        query = "INSERT INTO {} VALUES(".format(coll1)
        for element in attributes:
            if element in datapoint:
                attribute = datapoint[element]
            else:
                attribute = 'Empty'
            if element == fk:
                data_fk = attribute

            query = Modify.change_query(query, attribute)

        query = query[:-1] + ')'
        SQL.sqlexecute(conn, query,on_conflict=False)


        ignored_records+=insertion_order_products(conn, coll2, datapoint, attributes2,fk, data_fk)
    print('ignored order records:',ignored_records)

def insertion_order_products(conn,coll,datapoint,attributes,fk,data_fk):
    ignored_records=0
    count=0
    if 'order' in datapoint:
        if 'products' in datapoint['order']:
            for product in datapoint['order']['products']:
                query = "INSERT INTO {} VALUES(".format(coll)
                query = Modify.change_query(query, product['id'])
                query=Modify.change_query(query,data_fk)
                query = query[:-1] + ')'
                count+=1
                try:
                    SQL.sqlexecute(conn, query,on_conflict=False)
                except Exception as e:
                    ignored_records+=1
    return ignored_records

import psycopg2
import random
from pymongo import MongoClient
import SQL

def insertion_products(conn,data_input):
    count=0
    max_count=1000
    errors=0
    history_category=[]
    for datapoint in data_input:
        _id='Empty'
        brand='Empty'
        category='Empty'
        color='Empty'
        flavor='Empty'
        gender='Empty'
        herhaalaankopen='Empty'
        name='Empty'
        predict_out_of_stock_date='Empty'
        price='Empty'
        recommendable='Empty'
        stock='Empty'
        sub_category='Empty'
        sub_sub_category='Empty'

        try:
            if '_id' in datapoint:
                if type(datapoint['_id'])==str:
                    _id = datapoint['_id']
                    _id = _id.replace("'","")

            if 'brand' in datapoint:
                if type(datapoint['brand'])==str:
                    brand = datapoint['brand']
                    #print(_id+brand)
                    brand = brand.replace("'", "")

            if 'category' in datapoint:
                if type(datapoint['category']) == str:
                    category = datapoint['category']
                    category = category.replace("'", "")

            if category in history_category:
                continue
            else:
                history_category.append(category)

            if 'color' in datapoint:
                if type(datapoint['color']) == str:
                    color = datapoint['color']
                    color = color.replace("'", "")

            if 'flavor' in datapoint:
                if type(datapoint['flavor']) == str:
                    flavor = datapoint['flavor']
                    flavor = flavor.replace("'", "")

            if 'gender' in datapoint:
                if type(datapoint['gender']) == str:
                    gender = datapoint['gender']
                    gender = gender.replace("'", "")

            if 'herhaalaankopen' in datapoint:
                if type(datapoint['herhaalaankopen']) == str:
                    herhaalaankopen = datapoint['herhaalaankopen']
                    herhaalaankopen = herhaalaankopen.replace("'", "")

            if 'name' in datapoint:
                if type(datapoint['name']) == str:
                    name = datapoint['name']
                    name = name.replace("'", "")

            if 'predict_out_of_stock_date' in datapoint:
                if type(datapoint['predict_out_of_stock_date']) == str:
                    predict_out_of_stock_date = datapoint['predict_out_of_stock_date']
                    predict_out_of_stock_date = predict_out_of_stock_date.replace("'", "")

            if 'price' in datapoint:
                if 'selling_price' in datapoint['price']:
                    price = datapoint['price']['selling_price']

            if 'recommendable' in datapoint:
                if type(datapoint['recommendable']) == str:
                    recommendable = datapoint['recommendable']
                    recommendable = recommendable.replace("'", "")

            if 'stock' in datapoint:
                stock=datapoint['stock']

            if 'sub_category' in datapoint:
                if type(datapoint['sub_category']) == str:
                    sub_category = datapoint['sub_category']
                    sub_category = sub_category.replace("'", "")


            if 'sub_sub_category' in datapoint:
                if type(datapoint['sub_sub_category']) == str:
                    sub_sub_category = datapoint['sub_sub_category']
                    sub_sub_category = sub_sub_category.replace("'", "")


        except Exception as e:
            print('ERROR!!!!!!!!        ERROR!!!!!!!!       ERROR!!!!!!!!')
            print(e)
            print('id: ',_id)
            print('brand',brand)
            break

        sql_products = "INSERT INTO products VALUES('{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}','{}')".format(_id,brand,category,color,flavor,gender,herhaalaankopen,name,predict_out_of_stock_date,price,recommendable,sub_category,sub_sub_category)
        SQL.sqlexecute(conn,sql_products)

        insertion_stock(conn,stock,_id)

        if count>=max_count:
            break
        count+=1
    print("data products and stock has been inserted")

def insertion_stock(conn,stock,product_id):
    for i in stock:
        stock_level = 'Empty'
        date = 'Empty'
        stock_level = i['stock_level']
        date = i['date']

        sql_stock = "INSERT INTO stock VALUES('{}','{}','{}')".format(product_id, date, stock_level)
        SQL.sqlexecute(conn, sql_stock)

def insertion_visitors(conn,data_input):
    _id_list=[]

    for datapoint in data_input:
        _id='Empty'
        latest_activity='Empty'
        latest_visit='Empty'
        buids='Empty'

        if '_id' in datapoint:
            _id=datapoint['_id']

        if 'latest_activity' in datapoint:
            latest_activity=datapoint['latest_activity']

        if 'latest_visit' in datapoint:
            latest_visit=datapoint['latest_visit']

        if 'buids' in datapoint:
            buids = datapoint['buids']
            #buids exceed 2712 chars which is max for index
            buids=str(buids)
            buids=buids.replace("'","")

        sql = "INSERT INTO visitors VALUES('{}','{}','{}','{}')".format(_id, latest_activity, latest_visit, buids)
        SQL.sqlexecute(conn, sql)

        _id_list.append(_id)

    return _id_list

def insertion_sessions(conn,data_input,visitors_id_list):
    count=0
    for datapoint in data_input:
        _id='Empty'
        session_start='Empty'
        session_end='Empty'
        has_sale='Empty'
        cg_created='Empty'
        tg_created='Empty'
        visitors_id=visitors_id_list[count]

        if '_id' in datapoint:
            _id=datapoint['_id']

        if 'session_start' in datapoint:
            session_start = datapoint['session_start']

        if 'session_end' in datapoint:
            session_end = datapoint['session_end']

        if 'has_sale' in datapoint:
            has_sale = datapoint['has_sale']

        if 'cg_created' in datapoint:
            cg_created = datapoint['cg_created']

        if 'tg_created' in datapoint:
            tg_created = datapoint['tg_created']

        try:
            sql = "INSERT INTO sessions VALUES('{}','{}','{}','{}','{}','{}','{}')".format(_id,session_start,session_end,has_sale,cg_created,tg_created,visitors_id)
        except Exception as e:
            print(e)
            print('visitors id:',visitors_id)
            print('count: ',count)
            break

        SQL.sqlexecute(conn,sql)

        count+=1

import psycopg2
import random
from pymongo import MongoClient

def get_database(coll):
    client = MongoClient('mongodb://localhost:27017')
    db = client['dataset_hu']
    collection_student = db[coll]
    data = collection_student.find()
    return data

def get_data_products():
    products=get_database('products')
    products_list=[]
    for product in products:
        products_list.append(product)
    return products_list

def connect(database,username,password,host):
    conn = psycopg2.connect(database=database,user=username,password=password,host=host)
    conn.set_session(autocommit=True)
    return conn

def sqlexecute(conn,sql):
    cursor = conn.cursor()
    cursor.execute(sql)

def create_table(conn,coll,columns):
    #cursor.set_isolation_level(0)
    sqlexecute(conn,"CREATE TABLE {0}({1})".format(coll, columns))
    # cursor.execute("CREATE TABLE products({})".format(columns))
    print("Table products has been created")
    succes = True
    return succes

def insertion(conn,data_input):
    print('insertion is being called')
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
        sqlexecute(conn,sql_products)

        for i in stock:
            stock_level='Empty'
            date='Empty'
            stock_level=i['stock_level']
            date=i['date']

            sql_stock="INSERT INTO stock VALUES('{}','{}','{}')".format(_id,date,stock_level)
            sqlexecute(conn,sql_stock)
        if count>=max_count:
            break
        count+=1



def get_idList(list):
    max_count=100
    count=0
    id_list=[]
    history_category=[]
    for datapoint in products_list:
        if 'category' in datapoint:
            category=datapoint['category']
            if category in history_category:
                continue
            else:
                history_category.append(category)
        id_list.append(datapoint['_id'])
        if count>=max_count:
            break
        count+=1
    return id_list

def get_prices(id_list):
    prices=[]
    query="SELECT _id,price FROM products LIMIT 100;"
    sqlexecute(query)
    result=cursor.fetchall()
    print(result)
    for product in result:
        if product[0] in id_list:
            if product[1] != 'Empty':
                prices.append(float(product[1]))
    return prices

def get_average(price_list):
    som=0
    for price in price_list:
        som+=price
    average=som/len(price_list)
    return average

def choose_random_product(data):
    aantal_datapunten=len(data)
    random_index=random.randint(0,aantal_datapunten-1)
    random_product=data[random_index]
    return random_product

def get_PriceDifference(data,random_product):
    while True:
        try:
            price_randomProduct=random_product['price']['selling_price']
            break
        except:
            print('random product has changed')
            random_product=choose_random_product(data)

    priceDifference=0
    diffProduct=''
    for product in data:
        try:
            difference=product['price']['selling_price']-price_randomProduct
        except:
            pass
        try:
            abs_difference=float(str(difference).split('-')[-1])
        except:
            print(product)
            break
        if abs_difference>priceDifference:
            diffProduct=product
            priceDifference=abs_difference

    return priceDifference, product

#print(get_data_prodcuts())
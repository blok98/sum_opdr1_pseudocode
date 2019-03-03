import psycopg2
import random

def connect(database,username,password,host):
    conn = psycopg2.connect(database=database,user=username,password=password,host=host)
    conn.set_session(autocommit=True)
    return conn

def sqlexecute(conn,sql):
    cursor = conn.cursor()
    cursor.execute(sql)
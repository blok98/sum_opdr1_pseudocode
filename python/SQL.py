import psycopg2
import random

def connect(database,username,password,host):
    conn = psycopg2.connect(database=database,user=username,password=password,host=host)
    conn.set_session(autocommit=True)
    return conn

def sqlexecute(conn,sql,on_conflict):
    cursor = conn.cursor()
    if not on_conflict:
        cursor.execute(sql)
    else:
        cursor.execute(sql+" ON CONFLICT DO NOTHING;")


def delete_record(conn,table,constraint):
    sql="DELETE FROM {} WHERE {}".format(table,constraint)
    sqlexecute(conn,sql)


import SQL

def check(conn,coll):
    cursor=conn.cursor()
    cursor.execute("select * from information_schema.tables where table_name=%s", (coll,))
    exist = bool(cursor.rowcount)
    return exist

def drop_table(conn,coll):
    exist=check(conn,coll)
    if exist:
        query="DROP TABLE {}".format(coll)
        print('table {} is being dropped...'.format(coll))
        SQL.sqlexecute(conn,query)

def create_table(conn,coll,columns):
    #cursor.set_isolation_level(0)
    print("CREATE TABLE {0}({1})".format(coll, columns))
    SQL.sqlexecute(conn,"CREATE TABLE {0}({1})".format(coll, columns))
    # cursor.execute("CREATE TABLE products({})".format(columns))
    print("Table {} has been created".format(coll))
    succes = True
    return succes
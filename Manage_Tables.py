import SQL

def drop_table(conn,coll):
    query="DROP TABLE {}".format(coll)
    SQL.sqlexecute(conn,query)

def create_table(conn,coll,columns):
    #cursor.set_isolation_level(0)
    print("CREATE TABLE {0}({1})".format(coll, columns))
    SQL.sqlexecute(conn,"CREATE TABLE {0}({1})".format(coll, columns))
    # cursor.execute("CREATE TABLE products({})".format(columns))
    print("Table {} has been created".format(coll))
    succes = True
    return succes
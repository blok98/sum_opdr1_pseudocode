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
        SQL.sqlexecute(conn,query,on_conflict=False)

def create_table(conn,coll,columns_dict,primary_key,foreign_key):
    qry_columns = ''
    for column in columns_dict:
        qry_columns += column + ' '
        qry_columns += columns_dict[column] + ' '
        qry_columns += ','
    if primary_key:
        qry_columns += 'PRIMARY KEY({})'.format(primary_key)
        qry_columns += ','
    if foreign_key:
        for key in foreign_key:
            value=foreign_key[key]
            qry_columns += 'FOREIGN KEY({}) REFERENCES {}'.format(key,value)
            qry_columns += ','
    qry_columns = qry_columns[:-1]

    SQL.sqlexecute(conn,"CREATE TABLE {0}({1})".format(coll, qry_columns),on_conflict=False)
    # cursor.execute("CREATE TABLE products({})".format(columns))
    print("Table {} has been created".format(coll))
    succes = True
    return succes
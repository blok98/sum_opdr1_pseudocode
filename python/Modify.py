def change_query(query,value):
    attribute = str(value).replace("'", "")
    query += "'" + attribute + "'" + ","
    return query
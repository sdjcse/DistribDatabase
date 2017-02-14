
# Counting data in a table
def countData(openConnection,ratingsTableName):
    cursor = openConnection.cursor()
    queryString = "SELECT COUNT(*) FROM " + ratingsTableName
    cursor.execute(queryString)
    temp = cursor.fetchall()
    return temp[0][0]

def selectData(openConnection,columns,tableName,condColumns,conds,oper):
    cursor = openConnection.cursor()
    queryString = "SELECT "
    if columns is "*":
        queryString += columns + " "
    else:
        for x in columns:
            queryString += x+" "
    queryString += " FROM " + tableName
    if condColumns is not None:
        queryString += " WHERE "
        first = True
        for x,y,z in zip(condColumns,conds,oper):
            queryString += " " if (first) else " AND "
            queryString += x
            first = False
            if z is "ge":
                queryString += ">="
            elif z is "le":
                queryString += "<="
            elif z is "e":
                queryString += "="
            elif z is "g":
                queryString += ">"
            elif z is "l":
                queryString += "<"
            queryString += str(y)

    cursor.execute(queryString)
    records = cursor.fetchall()
    return records

def selectRRPartNum(tableName,openConnection):
    cursor = openConnection.cursor()
    queryString = "SELECT PARTITIONNUM FROM " + tableName
    cursor.execute(queryString)
    records = cursor.fetchall()
    return records[0][0]


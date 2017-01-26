from dbConnector import dbConnector

def readIt(tableName,value,columnName,isAll,selectColumns):
    cursor = dbConnector()
    if isAll:
        queryString = "SELECT * "
    else:
        queryString = "SELECT "
        for item in selectColumns:
            queryString = queryString + item + ","
        queryString = queryString[:-1]
    queryString += ' FROM ' + tableName
    cursor.execute(queryString)
    records = cursor.fetchall()
    return records

# Tester
# readIt('testtable','','',isAll=False,selectColumns=['name'])
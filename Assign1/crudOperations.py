import psycopg2
from dbConnector import dbConnector

def readIt(tableName,value,columnName,isAll,selectColumns):
    try:
        con = dbConnector()
        cursor = con.cursor()
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
    except psycopg2.DatabaseError, e:
        print 'Error %s' %e
    finally:
        con.close()
    return records

def insertAllVals(tableName,valueList):
    try:
        con = dbConnector()
        cursor = con.cursor()
        queryString = "INSERT INTO moviedetails VALUES (%s,%s,%s,%s);"
        cursor.execute(queryString,tuple(valueList))
        con.commit()
    except psycopg2.DatabaseError, e:
        print 'Error %s' %e
    finally:
        con.close()

def bulkInsert(fileName,tableName):
    try:
        con = dbConnector()
        cursor = con.cursor()
        cursor.copy_from(open(fileName),tableName,sep=':')
        con.commit()
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
    finally:
        con.close()

def copyTable(newTableName,oldTable,columns):
    try:
        con = dbConnector()
        cursor = con.cursor()
        queryString = "CREATE TABLE " + newTableName
        queryString += " AS SELECT "
        for element in columns:
            queryString += element + ","
        queryString = queryString[:-1]
        queryString += " " + oldTable
    except psycopg2.DatabaseError, e:
        print 'Error %s' % e
    finally:
        con.close()

        # Tester
# readIt('testtable','','',isAll=False,selectColumns=['name'])
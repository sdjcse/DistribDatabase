import psycopg2
from dbConnector import dbConnector
from Interface import *

def alterAfterLoading(openconnection,ratingstablename):
    queryString = """ ALTER TABLE """ + ratingstablename + """
                        DROP COLUMN dummy1,
                        DROP COLUMN dummy2,
                        DROP COLUMN dummy3,
                        DROP COLUMN timeFromStart"""
    cursor = openconnection.cursor()
    cursor.execute(queryString)
    cursor.close()
    openconnection.commit()

def createTableForLoading(openconnection,ratingstablename):
    con = getopenconnection()
    queryString = """ CREATE TABLE """ + ratingstablename + """(
        userid INTEGER,
        dummy1 CHAR,
        movieid INTEGER,
        dummy2 CHAR,
        rating REAL,
        dummy3 CHAR,
        timeFromStart BIGINT)
        """
    cursor = openconnection.cursor()
    cursor.execute(queryString)
    cursor.close()
    openconnection.commit()

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

def bulkInsert(fileName,tableName,openConnection):
    cursor = openConnection.cursor()
    cursor.copy_from(open(fileName),tableName,sep=':')
    openConnection.commit()

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

def countData(openConnection,ratingsTableName):
    cursor = openConnection.cursor()
    queryString = "SELECT COUNT(*) FROM " + ratingsTableName
    cursor.execute(queryString)
    temp = cursor.fetchall()
    return temp[0][0]

def createPartitionTables(tableName,openConnection,sourceTable,minrange,maxrange,includeMin):
    if includeMin:
        queryString = """CREATE TABLE <> AS
                         SELECT * FROM """ + sourceTable + """
                         WHERE RATING >= """ + str(minrange) + """
                         AND RATING <= """ + str(maxrange)
    else:
        queryString = """CREATE TABLE <> AS
                                 SELECT * FROM """ + sourceTable + """
                                 WHERE RATING > """ + str(minrange) + """
                                 AND RATING <= """ + str(maxrange)
    cursor = openConnection.cursor()
    cursor.execute(queryString.replace("<>",tableName))
    openConnection.commit()

def createPartitionTablesForRoundRobin(tableName,openConnection,sourceTable,numberOfPartition,nthPart):
    queryString = """ CREATE TABLE """ +tableName + """ AS
                      WITH TEMPTABLE AS
                      (
                        SELECT ROW_NUMBER() OVER() as FILTER,* FROM """ + sourceTable +"""
                      )
                      SELECT userid,movieid,rating
                      FROM TEMPTABLE
                      WHERE (FILTER-"""+str(nthPart) +""")%""" + str(numberOfPartition)+ """ = 0
                    """
    cursor = openConnection.cursor()
    cursor.execute(queryString)
    openConnection.commit()

def getPartCount(prefix,openConnection):
    queryString = """ SELECT COUNT(*) FROM pg_stat_user_tables
                      WHERE RELNAME LIKE '""" + prefix + """%'
                    """
    cursor = openConnection.cursor()
    cursor.execute(queryString)
    temp = cursor.fetchall()
    return temp[0][0]

def insertIntoTable(userid,movieid,rating,openConnection,table):
    queryString = """ INSERT INTO """ + table + """
                    VALUES(""" + str(userid)+ """ , """ + str(movieid) + """ , """ + str(rating) + """)
                """
    cursor = openConnection.cursor()
    cursor.execute(queryString)
    openConnection.commit()

def deleteTables(table,openConnection):
    queryString = "DROP TABLE " + table
    openConnection.cursor().execute(queryString)
    openConnection.commit()
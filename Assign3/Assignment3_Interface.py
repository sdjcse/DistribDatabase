#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import thread
import threading
from crudOperations import *

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'table1'
SECOND_TABLE_NAME = 'table2'
SORT_COLUMN_NAME_FIRST_TABLE = 'column1'
SORT_COLUMN_NAME_SECOND_TABLE = 'column2'
JOIN_COLUMN_NAME_FIRST_TABLE = 'column1'
JOIN_COLUMN_NAME_SECOND_TABLE = 'column2'
##########################################################################################################

threads = ["t1","t2","t3","t4","t5"]
threadList = []

class threadHelper(threading.Thread):
    def __init__(self, threadID, name,openconnection, InputTable,InputTable2,firstCol,secondCol, OutputTable, SortingColumnName,minVal,maxVal,firstThreadBool,sortBool):
        threading.Thread.__init__(self)
        self.threadID = threadID
        self.openconnection = openconnection
        self.InputTable = InputTable
        self.OutputTable = OutputTable
        self.InputTable2 = InputTable2
        self.firstCol = firstCol
        self.secondCol = secondCol
        self.SortingColumnName = SortingColumnName
        self.minVal = minVal
        self.maxVal = maxVal
        self.firstThreadBool = firstThreadBool
        self.name = name
        self.sortBool = sortBool
    def run(self):
        print "Starting " + self.name
        if self.sortBool:
            threadProcessor(self.name,self.openconnection,self.InputTable,self.OutputTable,self.SortingColumnName,self.minVal,self.maxVal,self.firstThreadBool)
        else:
            threadProcessorJoin(self.name,self.openconnection,self.InputTable,self.InputTable2,self.OutputTable,"",self.firstCol,self.secondCol,self.firstThreadBool,self.minVal,self.maxVal)
        print "Exiting " + self.name

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    createOutputTable(OutputTable, InputTable, openconnection)
    maxVal = getMaxMinOfAColumn(InputTable,openconnection,SortingColumnName,"MAX")
    minVal = getMaxMinOfAColumn(InputTable,openconnection,SortingColumnName,"MIN")
    interval = maxVal - minVal
    interval = float(interval)/5
    #data = selectFun(openconnection,InputTable,SortingColumnName,minVal,minVal+interval,True)
    #print data[0]
    count = 1
    #insertIntoTable(openconnection, OutputTable, data)
    for t in threads:
        if t is "t1":
            threadInst = threadHelper(count,t, openconnection, InputTable,"","","", OutputTable, SortingColumnName,minVal,minVal+interval,True,True)
        else:
            threadInst = threadHelper(count, t, openconnection, InputTable,"","","", OutputTable, SortingColumnName, minVal,minVal + interval, False,True)
        count+=1
        threadList.append(threadInst)
        threadInst.start()
        minVal = minVal+interval

def threadProcessor(threadName,openconnection,InputTable,OutputTable,SortingColumnName,firstVal,secondVal,firstThreadBool):
    data = selectFun(openconnection,InputTable,SortingColumnName,firstVal,secondVal,firstThreadBool)
    for t in threadList:
        if t.name is not threadName:
            t.join()
        else:
            break
    insertIntoTable(openconnection,OutputTable,data)

def threadProcessorJoin(threadName,openconnection,InputTable1,InputTable2,OutputTable,SortingColumnName,Table1Join,Table2Join,firstThreadBool,firstVal,secondVal):
    joinCols = [Table1Join,Table2Join]
    oper = ["g", "le"]
    if firstThreadBool:
        oper[0] = "ge"
    conds = [firstVal,secondVal]
    data = joinSelectData(openconnection, InputTable1,InputTable2,Table1Join,Table2Join,joinCols,conds,oper)
    insertIntoTable(openconnection, OutputTable, data)

def selectFun(openconnection,InputTable,SortingColumnName,firstVal,secondVal,firstThreadBool):
    sortCols = [SortingColumnName,SortingColumnName]
    oper = ["g","le"]
    if firstThreadBool:
        oper[0] = "ge"
    conds = [firstVal,secondVal]
    cursor = openconnection.cursor()
    colQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '<>'"
    cursor.execute(colQuery.replace("<>", InputTable.lower()))
    firstTabCols = cursor.fetchall()
    firstTabCols = [str(i[0]) for i in firstTabCols]
    data = selectData(openconnection, "*", InputTable, sortCols, conds, oper)
    data = sorted(data, key=lambda x: x[firstTabCols.index(SortingColumnName)])
    return data

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    createOutputTableJoin(OutputTable, InputTable1, InputTable2, openconnection, Table1JoinColumn,Table2JoinColumn)
    maxVal = max(getMaxMinOfAColumn(InputTable1, openconnection, Table1JoinColumn, "MAX"),getMaxMinOfAColumn(InputTable2, openconnection, Table2JoinColumn, "MAX"))
    minVal = min(getMaxMinOfAColumn(InputTable1, openconnection, Table1JoinColumn, "MIN"),getMaxMinOfAColumn(InputTable2, openconnection, Table2JoinColumn, "MIN"))
    interval = maxVal - minVal
    interval = float(interval) / 5
    count = 1
    for t in threads:
        if t is "t1":
            threadInst = threadHelper(count,t, openconnection, InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn, OutputTable, "",minVal,minVal+interval,True,False)
        else:
            threadInst = threadHelper(count,t, openconnection, InputTable1,InputTable2,Table1JoinColumn,Table2JoinColumn, OutputTable, "",minVal,minVal+interval,False,False)
        count += 1
        threadInst.start()
        minVal = minVal + interval


################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.commit()
    con.close()

# Donot change this function
def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
        # Creating Database ddsassignment3
        print "Creating Database named as ddsassignment3"
        createDB();

        # Getting connection to the database
        print "Getting connection from the ddsassignment3 database"
        con = getOpenConnection();

        # Calling ParallelSort
        print "Performing Parallel Sort"
        ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

        # Calling ParallelJoin
        print "Performing Parallel Join"
        ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

        # Saving parallelSortOutputTable and parallelJoinOutputTable on two files
        saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
        saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

        # Deleting parallelSortOutputTable and parallelJoinOutputTable
        deleteTables('parallelSortOutputTable', con);
        deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail

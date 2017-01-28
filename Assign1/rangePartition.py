from Interface import *
from crudOperations import *

def rangepartitionWork(ratingstablename, numberofpartitions, openconnection):
    commonPref = "range_part"
    replacestr = "<>"
    stringList = [commonPref+replacestr] * numberofpartitions
    newStr = [stringList[x].replace(replacestr,str(x)) for x in range(0,numberofpartitions)]
    startRange = 0
    interval = float(5)/numberofpartitions
    for x in range(0,numberofpartitions):
        if x is 0:
            createPartitionTables(newStr[x], openconnection, ratingstablename, startRange, startRange+interval, True)
        else:
            createPartitionTables(newStr[x], openconnection, ratingstablename, startRange, startRange + interval, False)
        startRange += interval

#rangepartition("allmovieratings",5,getopenconnection())

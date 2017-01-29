from crudOperations import *
import time

def deleteAll(openConnection):
    # Timer to measure the total time taken
    start = time.time()

    rangePref = "range_part"
    mainTab = "ratings"
    robinPref = "rrobin_part"
    rangeCount = getPartCount(rangePref,openConnection)
    robinCount = getPartCount(robinPref,openConnection)

    #deleteTables(mainTab,openConnection)
    for x in range(0,rangeCount):
        deleteTables(rangePref+str(x),openConnection)
    for x in range(0,robinCount):
        deleteTables(robinPref+str(x),openConnection)

    print "Deleted all partitions!"
    print time.time() - start

#unit tester
#deleteAll(getopenconnection())

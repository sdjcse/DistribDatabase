from crudOperations import *
import time

def rangeinsertWork(ratingstablename, userid, itemid, rating, openconnection):
    # Timer to measure the total time taken
    start = time.time()

    tabPref = "range_part"
    counter = getPartCount(tabPref,openconnection)
    interval = float(5)/counter
    startRange = 0
    for x in range(0,counter):
        tabName = tabPref+str(x)
        if ((startRange == 0 and rating >= startRange and rating <= startRange+interval)
            or (rating > startRange and rating <= startRange+interval)):
            insertIntoTable(userid,itemid,rating,openconnection,tabName)
            break
        startRange += interval
    print "Range Insertion successful!"
    print time.time() - start

#rangeinsert("testtable",1,123,4.0,getopenconnection())
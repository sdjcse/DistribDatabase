from crudOperations import *
import time

def roundrobininsertWork(ratingstablename, userid, itemid, rating, openconnection):
    prefix = "rrobin_part"
    # Timer to measure the total time taken
    start = time.time()
    counter = getPartCount(prefix,openconnection)
    flag = False
    value = -1
    for x in range(0,counter):
        tabName = prefix+str(x)
        if value==-1:
            value = countData(openconnection,tabName)
            continue
        if value > countData(openconnection,tabName):
            flag = True
            insertIntoTable(userid,itemid,rating,openconnection,tabName)
            break

    if not flag:
        insertIntoTable(userid, itemid, rating, openconnection, prefix+str(0))
    print "Insert Successful!"
    print time.time() - start


#roundrobininsert("testtable",1,123,4.5,getopenconnection())
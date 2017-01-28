from crudOperations import *
from Interface import *
import time

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    # Timer to measure the total time taken
    start = time.time()

    # create table to read data
    createTableForLoading(openconnection,ratingstablename)

    # using copy from to do bulk insert
    bulkInsert(ratingsfilepath,ratingstablename,openconnection)

    # dropping unnecessary columns
    alterAfterLoading(openconnection,ratingstablename)

    print "Inserting successful"
    print time.time() - start

#loadRatings("allmovieratings","data/ratings.dat",getopenconnection())

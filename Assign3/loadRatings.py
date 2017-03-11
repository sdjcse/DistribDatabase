from crudOperations import *
from Assignment3_Interface import *
import time

def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    # Timer to measure the total time taken
    start = time.time()

    # create table to read data
    # createTableForLoading(openconnection,ratingstablename)
    createGenreTableforLoading(openconnection,ratingstablename)

    # using copy from to do bulk insert
    bulkInsert(ratingsfilepath,ratingstablename,openconnection,"_")

    # dropping unnecessary columns
    # alterAfterLoading(openconnection,ratingstablename)

    print "Inserting successful"
    print time.time() - start

#loadRatings("allmovieratings","ratings.dat",getOpenConnection())
loadRatings("moviedetails","movies.dat",getOpenConnection())

from crudOperations import *
import time

def loadRatings():

    # Timer to measure the total time taken
    start = time.time()

    # create Database to read data

    # read data from the file
    fileName = "data/ratings.dat"
    tableName = "moviedetails"
    # using copy from to do bulk insert
    bulkInsert(fileName,tableName)
    #for line in open(fileName):
    #    insertAllVals("moviedetails",line.split("::"))

    # dropping unnecessary columns

    print "Inserting successful"
    print time.time() - start

loadRatings()
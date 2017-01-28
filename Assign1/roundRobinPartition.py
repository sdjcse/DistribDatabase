from crudOperations import *
import time

def roundrobinpartitionWork(ratingstablename, numberofpartitions, openconnection):
    start = time.time()
    partName = 'rrobin_part'
    for x in range(1,numberofpartitions+1):
        createPartitionTablesForRoundRobin(partName+str(x-1),openconnection,ratingstablename,numberofpartitions,x)
    print "Round Robin Partitions done!"
    print time.time() - start

#roundrobinpartition("allmovieratings",5,getopenconnection())
from crudOperations import *
from Assignment3_Interface import *

# Utility function to do unit testing over the functions written
def selector(openConnection,tableName,start_range,end_range,column_name):
    ParallelSort(tableName, column_name, "outputTable", openConnection)

#selector(getOpenConnection(),"moviedetails","","","movieid")

#createOutputTableJoin("dummyTest","moviedetails","allmovieratings",getOpenConnection(),"movieid","movieid")
#joinSelectData(getOpenConnection(),"moviedetails","allmovieratings","movieid","movieid",["movieid","movieid"],[0,1],["ge","le"])
ParallelJoin ("moviedetails", "allmovieratings", "movieid","movieid", "dummymain", getOpenConnection())
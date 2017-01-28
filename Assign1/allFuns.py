from deletePart import *
from loadRatings import *
from rangeInsert import *
from rangePartition import *
from roundRobinPartition import *
from rrobinInsert import *

def deletepartitionsandexit(openConnection):
    deleteAll(openConnection)

def loadratings(ratingstablename, filepath, openconnection):
    loadRatings(ratingstablename, filepath, openconnection)

def rangepartition(ratingstablename, n, openconnection):
    rangepartitionWork(ratingstablename, n, openconnection)

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    roundrobinpartitionWork(ratingstablename, numberofpartitions, openconnection)

def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
    roundrobininsertWork(ratingstablename, userid, itemid, rating, openconnection)

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    rangeinsertWork(ratingstablename, userid, itemid, rating, openconnection)
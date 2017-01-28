import psycopg2
import sys

con = None

def dbConnector():
    try:
        con = psycopg2.connect(database='postgres',user='postgres',password='',host='127.0.0.1')
        return con
    except psycopg2.DatabaseError, e:
        print 'Error %s' %e
        sys.exit(1)

# tester
def testerMethod():
    con = dbConnector()
    cursor = con.cursor()
    print "test"
    print "here"
    cursor.execute('SHOW work_mem')

#testerMethod()
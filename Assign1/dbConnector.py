import psycopg2
import sys

con = None

def dbConnector():
    try:
        con = psycopg2.connect(database='postgres',user='postgres',password='',host='127.0.0.1')
        cur = con.cursor()
        cur.execute('SELECT version()')
        ver = cur.fetchone()
        return cur
    except psycopg2.DatabaseError, e:
        print 'Error %s' %e
        sys.exit(1)

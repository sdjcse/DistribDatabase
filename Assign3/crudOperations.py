from numbers import Number

def selectData(openConnection,columns,tableName,condColumns,conds,oper):
    cursor = openConnection.cursor()
    queryString = "SELECT "
    if columns is "*":
        queryString += columns + " "
    else:
        for x in columns:
            queryString += x+" "
    queryString += " FROM " + tableName
    if condColumns is not None:
        queryString += " WHERE "
        first = True
        for x,y,z in zip(condColumns,conds,oper):
            queryString += " " if (first) else " AND "
            queryString += x
            first = False
            if z is "ge":
                queryString += ">="
            elif z is "le":
                queryString += "<="
            elif z is "e":
                queryString += "="
            elif z is "g":
                queryString += ">"
            elif z is "l":
                queryString += "<"
            queryString += str(y)

    cursor.execute(queryString)
    records = cursor.fetchall()
    return records


def createGenreTableforLoading(openConnection, tableName):
    queryString = """ CREATE TABLE """ + tableName + """(
        movieid INTEGER,
        moviename CHAR(100),
        genre CHAR(100)
        )
    """
    cursor = openConnection.cursor()
    cursor.execute(queryString)
    openConnection.commit()

def bulkInsert(fileName,tableName,openConnection,seperator):
    cursor = openConnection.cursor()
    cursor.copy_from(open(fileName),tableName,sep=seperator)
    openConnection.commit()

def getMaxMinOfAColumn(tableName,openConnection,columnName,maxMin):
    queryString = """SELECT """+maxMin+"""(""" + columnName + """) FROM """+ tableName
    cursor = openConnection.cursor()
    cursor.execute(queryString)
    temp = cursor.fetchall()
    return temp[0][0]

def createOutputTable(OutputTable,InputTable,openConnection):
    checkQuery = "select COUNT(*) from pg_tables where tablename = \'<>\' "
    checkQuery = checkQuery.replace("<>", OutputTable.lower())
    queryString = """CREATE TABLE """ + OutputTable + """ (LIKE """ + InputTable + """) """
    cursor = openConnection.cursor()
    cursor.execute(checkQuery)
    count = cursor.fetchone()[0]
    if count == 0:
        cursor.execute(queryString)
    else:
        print 'Table exists alread'
    openConnection.commit()

# This code assumes that column other than the join column does not have any similar names
def createOutputTableJoin(OutputTable,firstTab,secondTab,openConnection,firstCol,secondCol):
    cursor = openConnection.cursor()
    colQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '<>'"
    cursor.execute(colQuery.replace("<>",firstTab.lower()))
    firstTabCols = cursor.fetchall()
    cursor.execute(colQuery.replace("<>",secondTab.lower()))
    secondTabCols = cursor.fetchall()
    firstTabCols = [str(i[0]) for i in firstTabCols]
    secondTabCols = [str(i[0]) for i in secondTabCols]
    secondTabCols.remove(secondCol)
    firstTabCols.remove(firstCol)
    createQuery = """SELECT a."""+firstCol
    for i in firstTabCols:
        createQuery += ""","""+i
    for i in secondTabCols:
        createQuery += ""","""+i
    createQuery += """ INTO """ + OutputTable + """ FROM """ + firstTab + """ as a JOIN """+secondTab + """ as b ON a."""
    createQuery += firstCol + """=b.""" + secondCol
    cursor.execute(createQuery)
    truncQuery = "TRUNCATE "+OutputTable
    cursor.execute(truncQuery)
    openConnection.commit()

def insertIntoTable(openconnection,OutputTable,data):
    cursor = openconnection.cursor()
    countCols = "select count(*) from information_schema.columns where table_name='"+ OutputTable.lower() + "'"
    cursor.execute(countCols)
    temp = cursor.fetchall()
    questString = ""
    insList = []
    for tup in data:
        for i in range(len(tup)):
            if not isinstance(tup[i],Number):
                insList.append("\'"+tup[i]+"\'")
            else:
                insList.append(tup[i])
        queryString = "INSERT INTO " + OutputTable.lower() + " VALUES("
        for i in insList:
            queryString += str(i) + ","
        queryString =  queryString[:-1]
        queryString += ")"
        cursor.execute(queryString)
        insList = []
    openconnection.commit()


def joinSelectData(openConnection,firstTab,secondTab,firstCol,secondCol,condColumns,conds,oper):
    cursor = openConnection.cursor()
    colQuery = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '<>'"
    cursor.execute(colQuery.replace("<>", firstTab.lower()))
    firstTabCols = cursor.fetchall()
    cursor.execute(colQuery.replace("<>", secondTab.lower()))
    secondTabCols = cursor.fetchall()
    firstTabCols = [str(i[0]) for i in firstTabCols]
    secondTabCols = [str(i[0]) for i in secondTabCols]
    secondTabCols.remove(secondCol)
    firstTabCols.remove(firstCol)

    createQuery = """SELECT a.""" + firstCol + """ AS """ + firstCol
    for i in firstTabCols:
        createQuery += """,""" + i
    for i in secondTabCols:
        createQuery += """,""" + i
    createQuery += """ FROM """ + firstTab + """ as a,""" + secondTab + """ as b WHERE a."""
    createQuery += firstCol + """=b.""" + secondCol
    createQuery += """ AND"""

    if condColumns is not None:
        first = True
        for x,y,z in zip(condColumns,conds,oper):
            createQuery += " a." if (first) else " AND a."
            createQuery += x
            first = False
            if z is "ge":
                createQuery += ">="
            elif z is "le":
                createQuery += "<="
            elif z is "e":
                createQuery += "="
            elif z is "g":
                createQuery += ">"
            elif z is "l":
                createQuery += "<"
            createQuery += str(y)

    cursor.execute(createQuery)
    records = cursor.fetchall()
    return records

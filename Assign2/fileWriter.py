def writeIntoFile(filename,tabName,vals):
    with open(filename,"a") as myFile:
        for val in vals:
            myFile.write(tabName+","+str(val[0])+","+str(val[1])+","+str(val[2])+"\n")
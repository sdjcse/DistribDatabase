#!/usr/bin/python2.7
#
# Assignment3 Interface
# Name: 
#
import math
import re

from pymongo import MongoClient


def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    output = collection.find({"city":re.compile(cityToSearch,re.IGNORECASE)})
    target = open(saveLocation1,'w')
    for item in output:
        target.write(item["name"].encode('utf-8').encode('string_escape')+"$"+item["full_address"].encode('utf-8').encode('string_escape')+"$"+item["city"].encode('utf-8').encode('string_escape')+"$"+item["state"].encode('utf-8').encode('string_escape'))
        target.write("\n")
    target.close()

def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    output = collection.find()
    target = open(saveLocation2,'w')
    myLat = float(myLocation[0])
    myLon = float(myLocation[1])
    outList = []
    for item in output:
        categories = item["categories"]
        lat = item["latitude"]
        lon = item["longitude"]
        if maxDistance >= distance(lat,lon,myLat,myLon):
            if len(list(set(categories) & set(categoriesToSearch)))!=0:
                outList.append(item["name"].encode('utf-8'))
    for item in outList:
        target.write(item)
        target.write("\n")
    target.close()

def distance(lat2,lon2,lat1,lon1):
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delphi = math.radians(lat2-lat1)
    dellambda = math.radians(lon2-lon1)

    a = math.sin(delphi/2) * math.sin(delphi/2) + math.cos(phi1) * math.cos(phi2) * \
        math.sin(dellambda/2) * math.sin(dellambda/2)
    c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
    return 3959*c


DATABASE_NAME = "ddsassignment5"
COLLECTION_NAME = "businessCollection"
CITY_TO_SEARCH = "tempe"
MAX_DISTANCE = 100
CATEGORIES_TO_SEARCH = ["Fashion", "Food", "Cafes"]
MY_LOCATION = [ "33.331229700000002","-111.642224"] #[LATITUDE, LONGITUDE]
SAVE_LOCATION_1 = "findBusinessBasedOnCity.txt"
SAVE_LOCATION_2 = "findBusinessBasedOnLocation.txt"


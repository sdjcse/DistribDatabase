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
    pass

def distance(lat2,lon2,lat1,lon1):
    r = 3959; # miles
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delphi = math.radians(lat2-lat1)
    dellambda = math.radians(lon2-lon1)

    a = math.sin(delphi/2) * math.sin(delphi/2) + math.cos(phi1) * math.cos(phi2) * \
        math.sin(dellambda/2) * math.sin(dellambda/2)
    c = 2 * math.atan2(math.sqrt(a),math.sqrt(1-a))
    return r*c


DATABASE_NAME = "ddsassignment5"
COLLECTION_NAME = "businessCollection"
CITY_TO_SEARCH = "tempe"
MAX_DISTANCE = 100
CATEGORIES_TO_SEARCH = ["Fashion", "Food", "Cafes"]
MY_LOCATION = ["", ""] #[LATITUDE, LONGITUDE]
SAVE_LOCATION_1 = "findBusinessBasedOnCity.txt"
SAVE_LOCATION_2 = "findBusinessBasedOnLocation.txt"


if __name__ == '__main__':
    try:
        # Getting Connection from MongoDB
        conn = MongoClient('mongodb://localhost:27017/')

        # Creating a New DB in MongoDB
        print "Creating database in MongoDB named as " + DATABASE_NAME
        database = conn[DATABASE_NAME]

        # Creating a collection named businessCollection in MongoDB
        print "Creating a collection in " + DATABASE_NAME + " named as " + COLLECTION_NAME
        collection = database[COLLECTION_NAME]

        # Finding All Business name and address(full_address, city and state) present in CITY_TO_SEARCH
        print "Executing FindBusinessBasedOnCity function"
        FindBusinessBasedOnCity(CITY_TO_SEARCH, SAVE_LOCATION_1, collection)

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail


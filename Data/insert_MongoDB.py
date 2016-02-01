# coding: utf-8

# insert data into MongoDB
# uses package pymongo 3.1.1
# tested on MongoDB 3.0.6
# this will do a bulk insert that means that:
#   it will parse all the files and stores them in memory
#   and after that it will inset all the documents

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Development"

from os import listdir
from os.path import isfile, join
import sys
import pymongo
import json
from datetime import datetime


sensors = {
    'Ext_Tem': 'External Temperature',
    'Ext_Umi': 'External Humidity',
    'Ext_Vvi': 'External Wind Velocity',
    'Int_Tem': 'Internal Temperature',
    'Int_Umi': 'Internal Humidity',
    'Int_Pu1': 'Panouri Umbrire 1',
    'Int_Pu2': 'Panouri Umbrire 2'
}

units = {
    'Ext_Tem': '°C',
    'Ext_Umi': '%',
    'Ext_Vvi': 'km/h',
    'Int_Tem': '°C',
    'Int_Umi': '%',
    'Int_Pu1': '%',
    'Int_Pu2': '%'
}

def readJSONFile(filename):
    with open(filename) as data_file:    
        data = json.load(data_file)
    return data

def getData(folderpath):
    if not folderpath.endswith('/'):
        folderpath += '/'
    files = [ folderpath + f for f in listdir(folderpath) if isfile(join(folderpath,f)) ]
    corpus = []
    for f in files:
        corpus.append(readJSONFile(f))
    return corpus

def parseData(folderpath = 'data'):
    # give folder path to this function
    corpus = getData(folderpath = folderpath)
    dataset = []
    for elem in corpus:
        record = {}
        # format date date
        record["date"] = datetime.strptime(elem['record']['sdata'][0]['timestamp'], "%Y-%m-%d %H:%M:%S")
        record["greenhouse"] = {"name": str(elem['record']['sdata'][0]['name']), "city": "Cluj-Napoca", "county": "Cluj", "country": "Romania"}

        sensorList = []
        for sensor in elem['record']['sdata'][0]['sensors']:
            sensorDoc = {}
            sensorDoc["sensor"] = str(sensors[sensor['stype']])
            sensorDoc["stype"] = str(sensor['stype'])
            sensorDoc["value"] = str(sensor['value'])
            sensorDoc["units"] = str(units[sensor['stype']])
            sensorList.append(sensorDoc)
        record["sensors"] = sensorList
        dataset.append(record)
    return dataset


def insertData(hostname='localhost', port='27017', dbname='GreenhouseDB', folderpath = 'data'):
    try:
        dataset = parseData(folderpath=folderpath)
        client = pymongo.MongoClient(host=hostname, port=port)
        db = client[dbname]
        db.documents.insert(dataset, continue_on_error=True)
    except pymongo.errors.DuplicateKeyError:
        pass
    

# params
# hostname : MongoDB hostname
# port : MongoDB port
# dbname : database name
# folderpath : folder with the corpus

if __name__ == '__main__':
    hostname = sys.argv[1]
    port = int(sys.argv[2])
    dbname = sys.argv[3]
    folderpath = sys.argv[4] 

    insertData(hostname=hostname, port=port, dbname=dbname, folderpath=folderpath)



# coding: utf-8

__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Development"

import sqlite3
import json
import pymongo

sensors = {
    0: 'External Temperature',
    1: 'External Humidity',
    2: 'External Wind Velocity',
    3: 'Internal Temperature',
    4: 'Internal Humidity',
    5: 'Panouri Umbrire 1',
    6: 'Panouri Umbrire 2'
}

stype = {
    0: 'Ext_Tem',
    1: 'Ext_Umi',
    2: 'Ext_Vvi',
    3: 'Int_Tem',
    4: 'Int_Umi',
    5: 'Int_Pu1',
    6: 'Int_Pu1'
}

units = {
    0: '°C',
    1: '%',
    2: 'km/h',
    3: '°C',
    4: '%',
    5: '%',
    6: '%'
}

# insert data into MongoDB
# dataset - a list of dictionaries, the dataset that will be inserted in MongoDB
# hostname - MongoDB hostname or IP address given as a string
# port - MongoDB listening port
# dbname - the name of the database where the dataset will be inserted
# cleanDB - if true deletes everything from the document collection
def insertDataMongo(dataset, hostname='localhost', port=27017, dbname='GreenhouseDB', cleanDB=True):
    try:
        client = pymongo.MongoClient(host=hostname, port=port)
        db = client[dbname]
        if cleanDB:
            db.documents.delete_many({})
        db.documents.insert(dataset, continue_on_error=True)
    except pymongo.errors.DuplicateKeyError:
        pass

# create dataset from SQLite file, a list of dictionaries
# db_filename - the SQLite database file
def createDataset(db_filename):
    db_filename = db_filename
    dataset = []
    with sqlite3.connect(db_filename) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT s.Name, '2015-'||strftime('%m-%d %H:%M:%f', dl.RTS), dl.Ext_Tem, dl.Ext_Umi, dl.Ext_Vvi, dl.Int_Tem, dl.Int_Umi, dl.Int_Pu1, dl.Int_Pu2 FROM Sere s INNER JOIN DataLog dl on s.IDS = dl.IDS ORDER BY dl.RTS")
        rows = cursor. fetchall()
        cursor.close()
        i = 1
        for row in rows:

            sera_name = row[0]            
            d = {}
            # format date date
            d["greenhouse"] = {"name": row[0], "city": "Cluj-Napoca", "county": "Cluj", "country": "Romania"}
            d["date"] = row[1]
            

            l = []
            for j in range(0,7):
                d1 = {}
                d1["sensor"] = sensors[j]
                d1["stype"] = stype[j]
                d1["value"] = row[j+2]
                d1["units"] = units[j]
                l.append(d1)
            d["sensors"] = l
            dataset.append(d)
            i += 1
    return dataset

# create script for elastic search insert
# dataset - a list of dictionaries, the dataset that will be inserted in Elastic Search
# filename - name of the output filename
# port - elastic search port, default 9200
def createScript(dataset, filename='out_elastic.sh', port=9200):
    with open(filename, 'w') as elastic_script:
        elastic_script.write("#!/bin/bash\n")
        elastic_script.write("#__author__ = \"Ciprian-Octavian Truică\"\n")
        elastic_script.write("#__copyright__ = \"Copyright 2015, University Politehnica of Bucharest\"\n")
        elastic_script.write("#__license__ = \"GNU GPL\"\n")
        elastic_script.write("#__version__ = \"0.1\"\n")
        elastic_script.write("#__email__ = \"ciprian.truica@cs.pub.com\"\n")
        elastic_script.write("#__status__ = \"Production\"\n")
        idx = 1
        for elem in dataset:
            elastic_script.write("curl -XPUT 'http://localhost:"+str(port)+"/sensors/sensor/" + str(idx) + "' -d '" + json.dumps(elem) + "'\n")
            idx += 1
        elastic_script.close()
 

if __name__ == '__main__':
    dataset = createDataset(db_filename='CBM1501_Sere.db3')
    createScript(dataset=dataset)
    insertDataMongo(dataset=dataset)
    
    
    
    
    


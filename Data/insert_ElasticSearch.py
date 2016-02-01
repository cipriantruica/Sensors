# coding: utf-8

# create curl commands to add date in elasticsearch
__author__ = "Ciprian-Octavian Truică"
__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
__license__ = "GNU GPL"
__version__ = "0.1"
__email__ = "ciprian.truica@cs.pub.ro"
__status__ = "Development"

from os import listdir
from os.path import isfile, join
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

if __name__ == '__main__':
    # give folder path to this function
    corpus = getData(folderpath = 'data')
    port = 9200
    
    i = 1
    print "#!/bin/bash"
    print "#__author__ = \"Ciprian-Octavian Truică\""
    print "#__copyright__ = \"Copyright 2015, University Politehnica of Bucharest\""
    print "#__license__ = \"GNU GPL\""
    print "#__version__ = \"0.1\""
    print "#__email__ = \"ciprian.truica@cs.pub.com\""
    print "#__status__ = \"Production\""
    for elem in corpus:
        d = {}
        # format date date
        d["date"] = str(datetime.strptime(elem['record']['sdata'][0]['timestamp'], "%Y-%m-%d %H:%M:%S"))
        d["greenhouse"] = {"name": str(elem['record']['sdata'][0]['name']), "city": "Cluj-Napoca", "county": "Cluj", "country": "Romania"}

        l = []
        for sensor in elem['record']['sdata'][0]['sensors']:
            d1 = {}
            d1["sensor"] = str(sensors[sensor['stype']])
            d1["stype"] = str(sensor['stype'])
            d1["value"] = str(sensor['value'])
            d1["units"] = str(units[sensor['stype']])
            l.append(d1)
        d["sensors"] = l
        # print json.dumps(d)
        print "curl -XPUT 'http://localhost:"+str(port)+"/sensors/sensor/" + str(i) + "' -d '" + json.dumps(d) + "'"
        i += 1

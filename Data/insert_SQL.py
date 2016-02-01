# coding: utf-8

from os import listdir
from os.path import isfile, join
import json
from datetime import datetime


sensors = {
    'Ext_Tem': 1,
    'Ext_Umi': 2,
    'Ext_Vvi': 3,
    'Int_Tem': 4,
    'Int_Umi': 5,
    'Int_Pu1': 6,
    'Int_Pu2': 7
}

units = {
    '°C': 1,
    u'\xb0C': 1,
    'km/h': 2,
    '%': 3
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
    corpus = getData('data')
    s1 = set()
    s2 = set()
    for elem in corpus:
        time_received = datetime.strptime(elem['record']['sdata'][0]['timestamp'], "%Y-%m-%d %H:%M:%S")
        id_greenhouse = int(elem['record']['sdata'][0]['ids'])-1000
        
        for sensor in elem['record']['sdata'][0]['sensors']:
            id_sensor_unit = int(units[sensor['units']])
            id_sensor_type = int(sensors[sensor['stype']])
            sensor_value = int(sensor['value'])
            print "insert into sensors values(" + str(id_greenhouse) + ", " + str(id_sensor_unit) + ", " + str(id_sensor_type) + ", " + str(sensor_value) + ", to_date('" + str(time_sent) + "', 'YYYY-MM-DD HH24:MI:SS'), to_date('" + str(time_received) + "', 'YYYY-MM-DD HH24:MI:SS'));"

    
    

    
    
    
    

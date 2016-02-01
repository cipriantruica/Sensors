#!/bin/bash

#__author__ = "Ciprian-Octavian Truică"
#__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
#__license__ = "GNU GPL"
#__version__ = "0.1"
#__email__ = "ciprian.truica@cs.pub.com"
#__status__ = "Development"

#args:
#  1 - MongoDB hostname 
#  2 - MongoDB port
#  3 - Database name
#  4 - Folder with data
#example: python mongo_inset.py localhost 27017 GreenhouseDB data

HOST="localhost"
PORT=27017
DB="GreenhouseDB"
FOLDER="data"

python mongo_insert.py $HOST $PORT $DB $FOLDER



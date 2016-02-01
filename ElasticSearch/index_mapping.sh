#!/bin/bash

#__author__ = "Ciprian-Octavian Truică"
#__copyright__ = "Copyright 2015, University Politehnica of Bucharest"
#__license__ = "GNU GPL"
#__version__ = "0.1"
#__email__ = "ciprian.truica@cs.pub.com"
#__status__ = "Development"

curl -XPOST 'localhost:9200/sensors' -d '
{
    "mappings": {
        "sensor": { 
            "properties": { 
                "greenhouse": { 
                    "type" : "object",
                    "properties" : {
                        "name": { "type": "string" },
                        "city": { "type": "string" },
                        "county": { "type": "string" },
                        "country": { "type": "string" }
                    }
                },
                "date": { "type": "date",  "format": "yyyy-MM-dd HH:mm:ss" },
                "sensors": {
                    "properties": {
                        "units": { "type": "string"  },
                        "sensor": { "type": "string"  },
                        "stype": { "type": "string"  },
                        "value": { "type": "float"}
                    }
                }
            }
        }
    }
}'

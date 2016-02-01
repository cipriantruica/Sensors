# Sensors

### ElasticSearch
* index_mapping.sh - ElasticSearch field mapping

### SQLite 
* create_corpus.py - connect to a SQLite database file, creates a corpus, populates a MongoDB and creates a bash script to populate ElasticSearch (see ElasticSearch/index_mapping.sh)
* attach_db.sql - SQL and SQLite scripts

### SQL
* MySQL_create_tables.sql - script to create tables in MySQL
* Oracle_create_tables.sql - script to create tables in Oracle
* drops_tables.sql - drop tables Oracle/MySQL
* MySQL_Model.mwb - MySQL Model - for diagram
* Oracle_analytics_package.sql - package for OLAP
* Oracle_package_tests.sql - tests for the package

### Data
* insert_ElasticSearch.py - reads all the JSON files from a folder and creates a bash script 
* insert_SQL.py - reads all the JSON files from a folder and creates insert commands for the Sensors table Oracle (see SQL/Oracle_create_tables.sql)
* insert_MongoDB.py - reads all the JSON files from a folder and populates a MongoDB
* insert_MongoDB_run.sh - bash script to help run insert_MongoDB.py 

### Python Packages
* pip install -U sqlite3
* pip install -U json
* pip install -U pymongo


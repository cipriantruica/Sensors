-- to attach a file as database
-- ATTACH DATABASE 'file_path/file_name' as 'shema_name';
ATTACH DATABASE '/media/External_drive/Sensors_Project/SQLite/CBM1501_Sere.db3' as 'greenhouses';

-- to see all tables 
-- SELECT name FROM schema_name.sqlite_master WHERE type='table';
SELECT name FROM greenhouses.sqlite_master WHERE type='table';

-- to describe a table -- see its columns, data type, etc
-- PRAGMA table_info(table_name);
PRAGMA table_info(Sere);
PRAGMA table_info(Sensors);
PRAGMA table_info(Params);
PRAGMA table_info(DataLog);
PRAGMA table_info(Changes);
PRAGMA table_info(LastData);
PRAGMA table_info(Signals);



-- get descriptions for each sensor
SELECT * FROM Params;


-- get all the data we have in the JSON format
-- I don't know the date format, I suspect is just MM-DD H24:Mi:SS
SELECT s.Name, dl.RTS, dl.Ext_Tem, dl.Ext_Umi, dl.Ext_Vvi, dl.Int_Tem, dl.Int_Umi, dl.Int_Pu1, dl.Int_Pu2 FROM Sere s INNER JOIN DataLog dl on s.IDS = dl.IDS ORDER BY dl.RTS;

SELECT s.Name, '2015-'||strftime('%m-%d %H:%M:%f', dl.RTS), dl.Ext_Tem, dl.Ext_Umi, dl.Ext_Vvi, dl.Int_Tem, dl.Int_Umi, dl.Int_Pu1, dl.Int_Pu2 FROM Sere s INNER JOIN DataLog dl on s.IDS = dl.IDS ORDER BY dl.RTS;


-- other useless joins for now :)
SELECT s.Name, p.Field 
FROM Sensors s1
INNER JOIN Sere s ON s.IDS = s1.IDS
INNER JOIN Params p ON p.IDP = s1.IDP;

SELECT s.Name, p.Field FROM Sere s, Sensors s1, Params p WHERE p.IDP = s1.IDP AND s.IDS = s1.IDS;

SELECT julianday('%Y-%m-%d %H:%M:%S',min(RTS)) FROM DataLog;
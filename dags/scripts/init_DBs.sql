/*
this script used to initialize and create Data bases 
First we check if no DB with datawarehouse exist
Create the medallion arch 
*/

-- Create database if it doesn't exist
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = 'datawarehouse')
BEGIN
    CREATE DATABASE datawarehouse;
END

-- Create schemas in the datawarehouse database
IF NOT EXISTS (SELECT * FROM datawarehouse.sys.schemas WHERE name = 'bronze')
BEGIN
    EXEC('USE datawarehouse; CREATE SCHEMA bronze');
END

IF NOT EXISTS (SELECT * FROM datawarehouse.sys.schemas WHERE name = 'silver')
BEGIN
    EXEC('USE datawarehouse; CREATE SCHEMA silver');
END

IF NOT EXISTS (SELECT * FROM datawarehouse.sys.schemas WHERE name = 'gold')
BEGIN
    EXEC('USE datawarehouse; CREATE SCHEMA gold');
END

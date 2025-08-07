/*
this script used to initialize and create Data bases 
First we check if no DB with datawarehouse  exist
Creaate the madolline arch 
*/

USE master ; 

GO

create database if not exists datawarehouse ;

GO

use datawarehouse ;

Go

create schema bronze ;

Go

create schema silver ;

Go
  
create schema gold ;

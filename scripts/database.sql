/*
    This script is used to create the database and schemas for the data warehouse project.
*/


-- Before creating the database, we need to drop it if it exists

-- Kill all connections except yours, is someone is connected we can not drop the db
SELECT pg_terminate_backend(pid) --pg_terminate_backend is a function that terminates a backend connection
FROM pg_stat_activity --pg_stat_activity is a system table that contains information about all active connections to the database
WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid(); -- do not kill you own connection

DROP DATABASE IF EXISTS datawarehouse;

CREATE DATABASE datawarehouse;

CREATE SCHEMA IF NOT EXISTS bronze;

CREATE SCHEMA IF NOT EXISTS silver;

CREATE SCHEMA IF NOT EXISTS gold;

/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new PostgreSQL database named
    'data_warehouse'.

    If the database already exists, it is dropped and recreated.
    After the database is created, three schemas are added:
        - bronze
        - silver
        - gold

WARNING:
    Running this script will permanently delete the entire
    'data_warehouse' database if it exists, including all data
    and objects.

    Proceed with caution and ensure proper backups exist
    before running this script.
=============================================================
*/

-------------------------------------------------------------
-- STEP 1: CONNECT TO THE "postgres" DATABASE
--
-- IMPORTANT:
--   Run EACH statement below ONE AT A TIME (line by line).
--   Do NOT highlight and execute this entire section together.
--   PostgreSQL does NOT allow DROP/CREATE DATABASE inside
--   a transaction block.
-------------------------------------------------------------

-- 1️⃣ Terminate all active connections to the target database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'data_warehouse'
  AND pid <> pg_backend_pid();

-- 2️⃣ Drop the database if it already exists
DROP DATABASE IF EXISTS data_warehouse;

-- 3️⃣ Create the database
CREATE DATABASE data_warehouse;

-------------------------------------------------------------
-- STEP 2: RECONNECT TO THE "data_warehouse" DATABASE
--
-- After reconnecting, run the statements below normally.
-------------------------------------------------------------

-- Drop schemas if they exist (CASCADE removes all objects)
DROP SCHEMA IF EXISTS bronze CASCADE;
DROP SCHEMA IF EXISTS silver CASCADE;
DROP SCHEMA IF EXISTS gold CASCADE;

-- Create schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

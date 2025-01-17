USE ROLE ACCOUNTDMIN;
USE DATABASE DEMO_DB;
USE WAREHOUSE COMPUTE_WH;
 
CREATE OR REPLACE TABLE DEMO_DB.PUBLIC.PART
AS
SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCH_SF1"."PART";
 
SELECT * FROM PART
ORDER BY P_MFGR DESC;

UPDATE DEMO_DB.PUBLIC.PART
SET P_MFGR='Manufacturer#CompanyX'
WHERE P_MFGR='Manufacturer#5';
 
----> Note down query id here:
-- 01b9383d-3201-5d3f-000b-12da000540fa
 
SELECT * FROM PART
ORDER BY P_MFGR DESC;


SELECT * FROM  PART WHERE P_MFGR='Manufacturer#5';
SELECT * FROM  PART  at (OFFSET => -60*7);
SELECT * FROM PART before (statement =>'01b9383d-3201-5d3f-000b-12da000540fa');





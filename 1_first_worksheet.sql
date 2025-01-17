-- CREATE OR REPLACE WAREHOUSE FIRST_WH
-- WITH
-- WAREHOUSE_SIZE=XSMALL
-- MIN_CLUSTER_COUNT=1
-- MAX_CLUSTER_COUNT=3
-- COMMENT = 'This is our first warehouse'

-- ALTER warehouse FIRST_WH RESUME
-- ALTER WAREHOUSE first_wh SUSPEND
-- ALTER WAREHOUSE FIRST_WH SET WAREHOUSE_SIZE=SMALL
-- DROP WAREHOUSE FIRST_WH;
CREATE DATABASE FIRST_DB;
ALTER DATABASE FIRST_DB RENAME TO OUR_FIRST_DB;
CREATE TABLE "OUR_FIRST_DB"."PUBLIC"."LOAN_PAYMENT" (
  "Loan_ID" STRING,
  "loan_status" STRING,
  "Principal" STRING,
  "terms" STRING,
  "effective_date" STRING,
  "due_date" STRING,
  "paid_off_time" STRING,
  "past_due_days" STRING,
  "age" STRING,
  "education" STRING,
  "Gender" STRING);

  
 //Check that table is empy
 USE DATABASE OUR_FIRST_DB;

 SELECT * FROM LOAN_PAYMENT;
COPY INTO LOAN_PAYMENT
FROM s3://bucketsnowflakes3/Loan_payments_data.csv
file_format = (type = csv 
               field_delimiter = ',' 
               skip_header=1);

//Validate
 SELECT * FROM LOAN_PAYMENT;
    

  

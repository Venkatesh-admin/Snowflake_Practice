#### create a warehouse
CREATE OR REPLACE WAREHOUSE FIRST_WH
WITH
WAREHOUSE_SIZE=XSMALL
MIN_CLUSTER_COUNT=1
MAX_CLUSTER_COUNT=3
COMMENT = 'This is our first warehouse'

ALTER warehouse FIRST_WH RESUME
ALTER WAREHOUSE first_wh SUSPEND
ALTER WAREHOUSE FIRST_WH SET WAREHOUSE_SIZE=SMALL
DROP WAREHOUSE FIRST_WH;


### create a database and table
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
USE DATABASE OUR_FIRST_DB;
  
 #### CREATE STAGE

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
  url='s3://bucketsnowflakes3'
  credentials=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');

DESC STAGE MANAGE_DB.external_stages.aws_stage; 

LIST @aws_stage;

### COPY and OPTIONS

// Create table first
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.movie_titles (
  show_id STRING,
  type STRING,
  title STRING,
  director STRING,
  cast STRING,
  country STRING,
  date_added STRING,
  release_year STRING,
  rating STRING,
  duration STRING,
  listed_in STRING,
  description STRING )
  
  

// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE;
    
create or replace storage integration s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE 
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::529088268917:role/snowflakeaccessfrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake1712/csv/', 's3://snowflake1712/json/')
   COMMENT = 'This an optional comment' 
   
   
// See storage integration properties to fetch external_id so we can update it in S3
DESC integration s3_int;

--copy AWS IAM ROLE ARN AND External Id and attach in trust relationship of role created
    
 // Create stage object with integration object & file format object
CREATE OR REPLACE stage MANAGE_DB.external_stages.csv_folder
    URL = 's3://snowflake1712/csv/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat;

-- SELECT * FROM @MANAGE_DB.external_stages.csv_folder;

// Use Copy command       
COPY INTO OUR_FIRST_DB.PUBLIC.movie_titles
    FROM @MANAGE_DB.external_stages.csv_folder;
    
    
    
    
    
// Create file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.csv_fileformat
    type = csv
    field_delimiter = ','
    skip_header = 1
    null_if = ('NULL','null')
    empty_field_as_null = TRUE    
    FIELD_OPTIONALLY_ENCLOSED_BY = '"' ;   
    
    
SELECT * FROM OUR_FIRST_DB.PUBLIC.movie_titles;

// Taming the JSON file

// First query from S3 Bucket  
CREATE OR REPLACE file format MANAGE_DB.file_formats.json_file_format
    type = json;


CREATE OR REPLACE stage MANAGE_DB.external_stages.json_folder
    URL = 's3://snowflake1712/json/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.json_file_format;

SELECT * FROM @MANAGE_DB.external_stages.json_folder;



// Introduce columns 
SELECT 
$1:asin,
$1:helpful,
$1:overall,
$1:reviewText,
$1:reviewTime,
$1:reviewerID,
$1:reviewTime,
$1:reviewerName,
$1:summary,
$1:unixReviewTime
FROM @MANAGE_DB.external_stages.json_folder

// Format columns & use DATE function
SELECT 
$1:asin::STRING as ASIN,
$1:helpful as helpful,
$1:overall as overall,
$1:reviewText::STRING as reviewtext,
$1:reviewTime::STRING,
$1:reviewerID::STRING,
$1:reviewTime::STRING,
$1:reviewerName::STRING,
$1:summary::STRING,
DATE($1:unixReviewTime::int) as Revewtime
FROM @MANAGE_DB.external_stages.json_folder;

// Format columns & handle custom date 
SELECT 
$1:asin::STRING as ASIN,
$1:helpful as helpful,
$1:overall as overall,
$1:reviewText::STRING as reviewtext,
-- DATE_FROM_PARTS( <year, <month>, <day> )
$1:reviewTime::STRING,
$1:reviewerID::STRING,
$1:reviewTime::STRING,
$1:reviewerName::STRING,
$1:summary::STRING,
DATE($1:unixReviewTime::int) as Revewtime
FROM @MANAGE_DB.external_stages.json_folder;

// Use DATE_FROM_PARTS and see another difficulty
SELECT 
$1:asin::STRING as ASIN,
$1:helpful as helpful,
$1:overall as overall,
$1:reviewText::STRING as reviewtext,
DATE_FROM_PARTS( RIGHT($1:reviewTime::STRING,4), LEFT($1:reviewTime::STRING,2), SUBSTRING($1:reviewTime::STRING,4,2) ),
$1:reviewerID::STRING,
$1:reviewTime::STRING,
$1:reviewerName::STRING,
$1:summary::STRING,
DATE($1:unixReviewTime::int) as unixRevewtime
FROM @MANAGE_DB.external_stages.json_folder;


// Use DATE_FROM_PARTS and handle the case difficulty
SELECT 
$1:asin::STRING as ASIN,
$1:helpful as helpful,
$1:overall as overall,
$1:reviewText::STRING as reviewtext,
DATE_FROM_PARTS( 
  RIGHT($1:reviewTime::STRING,4), 
  LEFT($1:reviewTime::STRING,2), 
  CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=',' 
        THEN SUBSTRING($1:reviewTime::STRING,4,1) ELSE SUBSTRING($1:reviewTime::STRING,4,2) END),
$1:reviewerID::STRING,
$1:reviewTime::STRING,
$1:reviewerName::STRING,
$1:summary::STRING,
DATE($1:unixReviewTime::int) as UnixRevewtime
FROM @MANAGE_DB.external_stages.json_folder;


// Create destination table
CREATE OR REPLACE TABLE OUR_FIRST_DB.PUBLIC.reviews (
asin STRING,
helpful STRING,
overall STRING,
reviewtext STRING,
reviewtime DATE,
reviewerid STRING,
reviewername STRING,
summary STRING,
unixreviewtime DATE
)

// Copy transformed data into destination table
COPY INTO OUR_FIRST_DB.PUBLIC.reviews
    FROM (SELECT 
$1:asin::STRING as ASIN,
$1:helpful as helpful,
$1:overall as overall,
$1:reviewText::STRING as reviewtext,
DATE_FROM_PARTS( 
  RIGHT($1:reviewTime::STRING,4), 
  LEFT($1:reviewTime::STRING,2), 
  CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=',' 
        THEN SUBSTRING($1:reviewTime::STRING,4,1) ELSE SUBSTRING($1:reviewTime::STRING,4,2) END),
$1:reviewerID::STRING,
$1:reviewerName::STRING,
$1:summary::STRING,
DATE($1:unixReviewTime::int) Revewtime
FROM @MANAGE_DB.external_stages.json_folder)
   
    
// Validate results
SELECT * FROM OUR_FIRST_DB.PUBLIC.reviews    
    
    
## Overview of COPY Options in Snowflake

The `COPY INTO` command in Snowflake is a powerful tool for loading data from various sources into tables or unloading data to specified locations. Below are detailed notes on key parameters and options available within this command, including file formats, error handling, validation modes, size limits, and more.

### File Format

- **FILE_FORMAT**: This option allows you to specify the format of the files being loaded or unloaded. You can define it using:
  - **FORMAT_NAME**: A pre-defined file format name.
  - **TYPE**: Specify the type of file format such as CSV, JSON, or PARQUET, along with any relevant options for that format.

### Error Handling

- **ON_ERROR**: This parameter dictates how Snowflake should respond when it encounters an error during the copy operation. The options include:
  - **ABORT_STATEMENT**: Abort the entire operation upon encountering an error.
  - **CONTINUE**: Skip the problematic rows and continue processing.
  - **SKIP_FILE**: Skip the entire file if any errors occur.

### Validation Mode

- **VALIDATION_MODE**: This option is used to control how data validation is performed during the loading process. It can be set to:
  - **RETURN_ROWS**: Returns rows that would be loaded without actually performing the load. This is useful for testing and validation purposes.

### Size Limit

- **SIZE_LIMIT**: This parameter can be used to specify the maximum size of files being processed. It helps in managing large datasets by limiting the amount of data loaded in a single operation.

### Return Failed Rows

- **RETURN_FAILED**: When enabled, this option allows you to return rows that failed during processing for further analysis or correction.

### Truncated Columns

- **TRUNCATED_COLUMNS**: This option can be used to specify how to handle columns that may exceed their defined length in the target table. You can choose to truncate these columns or raise an error based on your requirements.

### Force Option

- **FORCE**: When this option is set, it forces the operation to proceed even if there are warnings or issues that would typically halt execution. This can be useful in scenarios where you want to ensure that as much data as possible is processed despite potential issues.

### Example Syntax

Here’s a basic example of how these options might be structured within a `COPY INTO` command:

```sql
COPY INTO my_table
FROM @my_stage
FILE_FORMAT = (TYPE = 'CSV')
ON_ERROR = 'CONTINUE'
VALIDATION_MODE = 'RETURN_ROWS'
SIZE_LIMIT = 1000000
RETURN_FAILED = TRUE
TRUNCATED_COLUMNS = TRUE
FORCE = TRUE;
```

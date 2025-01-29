# Snowflake Data Loading and Warehouse Management

## Overview
This document provides a detailed guide on creating and managing a Snowflake warehouse, databases, tables, and stages. It also covers data loading using the `COPY INTO` command with various options for error handling, validation, and formatting.

## Warehouse Management

### Creating a Warehouse
```sql
CREATE OR REPLACE WAREHOUSE FIRST_WH
WITH
WAREHOUSE_SIZE=XSMALL
MIN_CLUSTER_COUNT=1
MAX_CLUSTER_COUNT=3
COMMENT = 'This is our first warehouse';
```

### Modifying a Warehouse
```sql
ALTER WAREHOUSE FIRST_WH RESUME;
ALTER WAREHOUSE FIRST_WH SUSPEND;
ALTER WAREHOUSE FIRST_WH SET WAREHOUSE_SIZE=SMALL;
```

### Dropping a Warehouse
```sql
DROP WAREHOUSE FIRST_WH;
```

## Database and Table Management

### Creating a Database
```sql
CREATE DATABASE FIRST_DB;
```

### Renaming a Database
```sql
ALTER DATABASE FIRST_DB RENAME TO OUR_FIRST_DB;
```

### Creating a Table
```sql
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
  "Gender" STRING
);
```

### Selecting Database
```sql
USE DATABASE OUR_FIRST_DB;
```

## External Stages and File Formats

### Creating an External Stage
```sql
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_stage
  URL='s3://bucketsnowflakes3'
  CREDENTIALS=(aws_key_id='ABCD_DUMMY_ID' aws_secret_key='1234abcd_key');
```

### Describing and Listing Stage Contents
```sql
DESC STAGE MANAGE_DB.external_stages.aws_stage;
LIST @aws_stage;
```

### Creating a File Format
```sql
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.csv_fileformat
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL','null')
    EMPTY_FIELD_AS_NULL = TRUE;
```

### Creating a Storage Integration
```sql
CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::529088268917:role/snowflakeaccessfrole'
  STORAGE_ALLOWED_LOCATIONS = ('s3://snowflake1712/csv/', 's3://snowflake1712/json/')
  COMMENT = 'This is an optional comment';
```

### Creating a Stage with Integration
```sql
CREATE OR REPLACE STAGE MANAGE_DB.external_stages.csv_folder
    URL = 's3://snowflake1712/csv/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.csv_fileformat;
```

## Copying Data into Snowflake Tables

### Copying Data from a Stage
```sql
COPY INTO OUR_FIRST_DB.PUBLIC.movie_titles
    FROM @MANAGE_DB.external_stages.csv_folder;
```

### Handling JSON Data
```sql
CREATE OR REPLACE FILE FORMAT MANAGE_DB.file_formats.json_file_format
    TYPE = JSON;

CREATE OR REPLACE STAGE MANAGE_DB.external_stages.json_folder
    URL = 's3://snowflake1712/json/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = MANAGE_DB.file_formats.json_file_format;
```

### Querying JSON Data
```sql
SELECT
$1:asin::STRING AS ASIN,
$1:helpful AS helpful,
$1:overall AS overall,
$1:reviewText::STRING AS reviewtext,
DATE_FROM_PARTS(
  RIGHT($1:reviewTime::STRING,4),
  LEFT($1:reviewTime::STRING,2),
  CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=','
        THEN SUBSTRING($1:reviewTime::STRING,4,1) ELSE SUBSTRING($1:reviewTime::STRING,4,2) END)
AS reviewtime,
$1:reviewerID::STRING AS reviewerid,
$1:reviewerName::STRING AS reviewername,
$1:summary::STRING AS summary,
DATE($1:unixReviewTime::int) AS unixreviewtime
FROM @MANAGE_DB.external_stages.json_folder;
```

### Creating a Table for JSON Data
```sql
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
);
```

### Copying Transformed Data into Table
```sql
COPY INTO OUR_FIRST_DB.PUBLIC.reviews
    FROM (SELECT
$1:asin::STRING AS ASIN,
$1:helpful AS helpful,
$1:overall AS overall,
$1:reviewText::STRING AS reviewtext,
DATE_FROM_PARTS(
  RIGHT($1:reviewTime::STRING,4),
  LEFT($1:reviewTime::STRING,2),
  CASE WHEN SUBSTRING($1:reviewTime::STRING,5,1)=','
        THEN SUBSTRING($1:reviewTime::STRING,4,1) ELSE SUBSTRING($1:reviewTime::STRING,4,2) END) AS reviewtime,
$1:reviewerID::STRING AS reviewerid,
$1:reviewerName::STRING AS reviewername,
$1:summary::STRING AS summary,
DATE($1:unixReviewTime::int) AS unixreviewtime
FROM @MANAGE_DB.external_stages.json_folder);
```

## COPY INTO Command Overview

The `COPY INTO` command in Snowflake is used for loading and unloading data. Key options include:

- **FILE_FORMAT**: Specifies the file format (CSV, JSON, etc.).
- **ON_ERROR**: Controls error handling (ABORT_STATEMENT, CONTINUE, SKIP_FILE).
- **VALIDATION_MODE**: Validates data before loading (RETURN_ROWS for test runs).
- **SIZE_LIMIT**: Limits file sizes processed in one batch.
- **RETURN_FAILED**: Returns rows that failed processing.
- **TRUNCATED_COLUMNS**: Manages column truncation.
- **FORCE**: Forces execution despite warnings.

### Example COPY INTO Usage
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

## Conclusion
This guide provides a comprehensive approach to managing Snowflake warehouses, databases, tables, and data loading processes. By leveraging Snowflake's powerful staging, file format configurations, and error handling options, you can efficiently ingest and process large datasets.


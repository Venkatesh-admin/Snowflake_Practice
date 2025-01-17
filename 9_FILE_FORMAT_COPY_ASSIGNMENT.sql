CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_file_format_stage
    url='s3://snowflake-assignments-mc/fileformat/';
List @aws_file_format_stage;
DESCRIBE STAGE aws_file_format_stage;

CREATE OR REPLACE SCHEMA MANAGE_DB.file_formats;

// Creating file format object
CREATE OR REPLACE file format MANAGE_DB.file_formats.my_file_format;
ALTER file format MANAGE_DB.file_formats.my_file_format
    SET SKIP_HEADER = 1;
DESCRIBE file format MANAGE_DB.file_formats.my_file_format;
ALTER file format MANAGE_DB.file_formats.my_file_format
    SET FIELD_DELIMITER = '|';
create or replace TABLE EXERCISE_DB.PUBLIC.CUSTOMERS (
	ID NUMBER(38,0),
	FIRST_NAME VARCHAR(16777216),
	LAST_NAME VARCHAR(16777216),
	EMAIL VARCHAR(16777216),
	AGE NUMBER(38,0),
	CITY VARCHAR(16777216)
);
COPY INTO EXERCISE_DB.PUBLIC.CUSTOMERS
    FROM @MANAGE_DB.external_stages.aws_file_format_stage
    file_format= (FORMAT_NAME=MANAGE_DB.file_formats.my_file_format)
    files = ('customers4.csv')
    ON_ERROR = 'CONTINUE'; 

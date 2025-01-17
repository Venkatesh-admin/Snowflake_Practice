CREATE OR REPLACE STAGE MANAGE_DB.external_stages.aws_assignment_stage
    url='s3://snowflake-assignments-mc/loadingdata/';

//List data

List @aws_assignment_stage;
DESCRIBE STAGE aws_assignment_stage;

//Load the data in the existing customers table using the COPY command
COPY INTO EXERCISE_DB.PUBLIC.CUSTOMERS FROM
   @MANAGE_DB.EXTERNAL_STAGES.AWS_ASSIGNMENT_STAGE 
    file_format=(type = csv field_delimiter=';' skip_header=1)
    pattern = '.*customers.*' ;
    
COPY INTO EXERCISE_DB.PUBLIC.CUSTOMERS FROM
   @MANAGE_DB.EXTERNAL_STAGES.AWS_ASSIGNMENT_STAGE 
    file_format=(type = csv field_delimiter=';' skip_header=1)
    files=('customers3.csv');

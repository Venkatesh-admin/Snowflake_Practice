CREATE OR REPLACE Table EXERCISE_DB.public.employees(
  customer_id int,

  first_name varchar(50),

  last_name varchar(50),

  email varchar(50),

  age int,

  department varchar(50)
);

create or replace stage exercise_db.public.validation_stage 
url='s3://snowflake-assignments-mc/copyoptions/example1';

List @exercise_db.public.validation_stage; 

create or replace file format exercise_db.public.validation_file_format  SKIP_HEADER=1;

DESCRIBE file format exercise_db.public.validation_file_format;

COPY into exercise_db.public.employees from @exercise_db.public.validation_stage
file_format= (FORMAT_NAME=exercise_db.public.validation_file_format)
    pattern='.*employees.*'
    VALIDATION_MODE = RETURN_ERRORS;
SELECT * from exercise_db.public.employees;
COPY into exercise_db.public.employees from @exercise_db.public.validation_stage
file_format= (FORMAT_NAME=exercise_db.public.validation_file_format)
    pattern='.*employees.*'
    ON_ERROR='CONTINUE'



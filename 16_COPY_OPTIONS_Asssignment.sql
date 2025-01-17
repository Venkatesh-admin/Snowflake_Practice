create or replace table exercise_db.public.employees (
customer_id int,

first_name varchar(50),

last_name varchar(50),

email varchar(50),

age int,

department varchar(50)
);

create stage exercise_db.public.copy_options_stage
url='s3://snowflake-assignments-mc/copyoptions/example2';

LIST @copy_options_stage;

copy into exercise_db.public.employees from 
@copy_options_stage
file_format= (type = csv field_delimiter=',' skip_header=1)
VALIDATION_MODE = RETURN_ERRORS;

copy into exercise_db.public.employees from 
@copy_options_stage
file_format= (type = csv field_delimiter=',' skip_header=1)
TRUNCATECOLUMNS=TRUE;
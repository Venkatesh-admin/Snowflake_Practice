-- Create a stage object that is pointing to 's3://snowflake-assignments-mc/unstructureddata/'
create or replace schema exercise_db.aws_stage;
create or replace stage exercise_db.aws_stage.load_json_stage
url='s3://snowflake-assignments-mc/unstructureddata/';
List @load_json_stage;

-- Create a file format object that is using TYPE = JSON
create or replace schema exercise_db.file_formats;
create file format exercise_db.file_formats.load_json_format
TYPE=JSON;
describe file format exercise_db.file_formats.load_json_format;

-- Create a table called JSON_RAW with one column
create or replace table exercise_db.public.JSON_RAW (Raw variant);

-- Copy the raw data in the JSON_RAW table using the file format object and stage object
copy into exercise_db.public.JSON_RAW 
from @load_json_stage 
file_format=exercise_db.file_formats.load_json_format;


select * from exercise_db.public.JSON_RAW;
select RAW:first_name::string as first_name,
       RAW:last_name::string as last_name,
       RAW:Skills as skills 
FROM exercise_db.public.JSON_RAW;


-- select the attributes
select RAW:first_name::string as first_name,
       RAW:last_name::string as last_name,
       RAW:Skills[0]::string as skills_1, 
       RAW:Skills[0]::string as skills_2
FROM exercise_db.public.JSON_RAW;

create or replace table exercise_db.public.json_table(
first_name string,
last_name string,
skills_1 string,
skills_2 string
);

-- insert into a table from the json raw table
Insert into exercise_db.public.json_table
select RAW:first_name::string as first_name,
       RAW:last_name::string as last_name,
       RAW:Skills[0]::string as skills_1, 
       RAW:Skills[0]::string as skills_2
FROM exercise_db.public.JSON_RAW;

-- Get skills of Florina
select * from exercise_db.public.json_table where first_name='Florina';


       

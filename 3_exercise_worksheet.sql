create data EXERCISE_DB;
create table CUSTOMERS (ID INT,

first_name varchar,

last_name varchar,

email varchar,

age int,

city varchar);

COPY INTO CUSTOMERS 
from s3://snowflake-assignments-mc/gettingstarted/customers.csv
file_format = (type = csv 
               field_delimiter = ',' 
               skip_header=1);
select count(*) from customers;


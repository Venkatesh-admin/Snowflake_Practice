CREATE DATABASE DEMO_DB;
USE DATABASE DEMO_DB;
-- create integration object that contains the access information
CREATE STORAGE INTEGRATION azure_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '985adafb-97bd-46f6-92d8-8846e167a468'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowflakepractice1702.blob.core.windows.net/snowflake/world-happiness-report-2021.csv', 'azure://snowflakepractice1702.blob.core.windows.net/snowflake/CarModels.json');

  -- https://snowflakepractice1702.blob.core.windows.net/snowflake/CarModels.json
-- https://snowflakepractice1702.blob.core.windows.net/snowflake/world-happiness-report-2021.csv
  
-- Describe integration object to provide access
DESC STORAGE integration azure_integration;

  
---- Create file format & stage objects ----

-- create file format
create or replace file format demo_db.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage demo_db.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://snowflakepractice1702.blob.core.windows.net/snowflake/world-happiness-report-2021.csv'
    FILE_FORMAT = fileformat_azure;
    

-- list files
LIST @demo_db.public.stage_azure;


---- Query files & Load data ----

--query files
SELECT 
$1,
$2,
$3,
$4,
$5,
$6,
$7,
$8,
$9,
$10,
$11,
$12,
$13,
$14,
$15,
$16,
$17,
$18,
$19,
$20
FROM @demo_db.public.stage_azure;


create or replace table happiness (
    country_name varchar,
    regional_indicator varchar,
    ladder_score number(4,3),
    standard_error number(4,3),
    upperwhisker number(4,3),
    lowerwhisker number(4,3),
    logged_gdp number(5,3),
    social_support number(4,3),
    healthy_life_expectancy number(5,3),
    freedom_to_make_life_choices number(4,3),
    generosity number(4,3),
    perceptions_of_corruption number(4,3),
    ladder_score_in_dystopia number(4,3),
    explained_by_log_gpd_per_capita number(4,3),
    explained_by_social_support number(4,3),
    explained_by_healthy_life_expectancy number(4,3),
    explained_by_freedom_to_make_life_choices number(4,3),
    explained_by_generosity number(4,3),
    explained_by_perceptions_of_corruption number(4,3),
    dystopia_residual number (4,3));
    
    
COPY INTO HAPPINESS
FROM @demo_db.public.stage_azure;

SELECT * FROM HAPPINESS;

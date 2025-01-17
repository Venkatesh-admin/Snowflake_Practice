create or replace database snowpipe;
create or replace storage integration azure_integration 
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '985adafb-97bd-46f6-92d8-8846e167a468'
  STORAGE_ALLOWED_LOCATIONS = ('azure://snowflakepipe709.blob.core.windows.net/snowpipecsv');
describe storage integration azure_integration;

create or replace file format snowpipe.public.fileformat_azure
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;

-- create stage object
create or replace stage snowpipe.public.stage_azure
    STORAGE_INTEGRATION = azure_integration
    URL = 'azure://snowflakepipe709.blob.core.windows.net/snowpipecsv'
    FILE_FORMAT = fileformat_azure;

List @snowpipe.public.stage_azure; 


CREATE OR REPLACE NOTIFICATION INTEGRATION snowpipe_event
ENABLED=TRUE
TYPE=QUEUE
NOTIFICATION_PROVIDER=AZURE_STORAGE_QUEUE
AZURE_STORAGE_QUEUE_PRIMARY_URI= 'https://snowflakepipe709.queue.core.windows.net/snowpipequeue'
AZURE_TENANT_ID = '985adafb-97bd-46f6-92d8-8846e167a468';

DESCRIBE NOTIFICATION INTEGRATION snowpipe_event;
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
FROM @snowpipe.public.stage_azure;

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
FROM @snowpipe.public.stage_azure;

SELECT * FROM HAPPINESS;

TRUNCATE table happiness;

CREATE OR REPLACE pipe azure_pipe
auto_ingest = TRUE
INTEGRATION='SNOWPIPE_EVENT'
AS
COPY INTO snowpipe.public.happiness
FROM @snowpipe.public.stage_azure; 

SELECT * FROM HAPPINESS;









-- Frosty Friday Challenge
-- Week 3 - Intermediate - Metadata Queries


-------------------------------
-- Environment Configuration

use database SG_FROSTY_FRIDAY;


create or replace schema WEEK_3;

use schema WEEK_3;

-------------------------------
--creating a stage

create or replace stage STG_WEEK_3
  URL = 's3://frostyfridaychallenges/challenge_3/'
;

-- View files in stage
list @STG_WEEK_3;

--Investigate the files in stage
--creating a file format to load the data into a single row to see and understand the data

create or replace file format FF_SINGLE_FIELD
    type = CSV
    field_delimiter = NONE
    record_delimiter = NONE
    skip_header = 0 
;

--query the date using the single field file format to understand the data 

select 
    METADATA$FILENAME::STRING as FILE_NAME
  , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
  , $1::VARIANT as CONTENTS
from @STG_WEEK_3
  (file_format => 'FF_SINGLE_FIELD')
order by 
    FILE_NAME
  , ROW_NUMBER
;

-- Other than the first file i.e keywords file, each
-- file seems to match the same format.(id,firstname,lastname,catchphrase and timestamp)

-- We can ingest all files other than keywords
-- into a destination, and import keywords
-- into a separate table.

-- Also, the file format looks like standard csv

--ingest the files into a table..creating FF_SINGLE_INGESTION 

-- Create the appropriate file format
create or replace file format FF_CSV_INGESTION
    type = CSV
    field_delimiter = ','
    record_delimiter = '\n'
    skip_header = 1

    
--creating a table to load the data from stage
-- Create a table in which to land the data
create or replace table RAW_DATA (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , ID STRING
  , FIRST_NAME STRING
  , LAST_NAME STRING
  , CATCH_PHRASE STRING
  , TIMESTAMP_RAW STRING
)
;   


-- Ingesting the data in RAW_DATA table
COPY INTO RAW_DATA
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::string as ID
    , $2::string as FIRST_NAME
    , $3::string as LAST_NAME
    , $4::string as CATCH_PHRASE
    , $5::string as TIMESTAMP_RAW
  from @STG_WEEK_3/week3
    (file_format => 'FF_CSV_INGESTION')
)
;


-- Create a table in which to land the data
create or replace table KEYWORDS (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , KEYWORD STRING
  , ADDED_BY STRING
  , NONSENSE STRING
)
;


COPY INTO KEYWORDS
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::string as KEYWORD
    , $2::string as ADDED_BY
    , $3::string as NONSENSE
  from @STG_WEEK_3/keywords
    (file_format => 'FF_CSV_INGESTION')
)
;

-- Query the results
SELECT *
FROM RAW_DATA
ORDER BY 
    FILE_NAME
  , ROW_NUMBER
;

SELECT *
FROM KEYWORDS
ORDER BY 
    FILE_NAME
  , ROW_NUMBER
;


-- View of ingested keyword files

create or replace view INGESTED_KEYWORD_FILES
as
select 
    FILE_NAME
  , count(*) as NUMBER_OF_ROWS
from RAW_DATA
where exists (
  SELECT 1 
  FROM KEYWORDS
  WHERE CONTAINS(RAW_DATA.FILE_NAME, KEYWORDS.KEYWORD)
)
group by
    FILE_NAME
;


-- Query the view
select * from INGESTED_KEYWORD_FILES
order by 1
;

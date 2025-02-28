create or replace database SG_FROSTY_FRIDAY;

create or replace schema WEEK_1;
use schema WEEK_1;


--creating a stage 
create or replace stage WEEK_1
    URL = 's3://frostyfridaychallenges/challenge_1/'
;

--listing the stage contents

list @WEEK_1;


--creating a file format to load the data into a single row to see and understand the data

create or replace file format FF_SINGLE_FIELD
    type = CSV
    field_delimiter = NONE
    record_delimiter = NONE
    skip_header = 0 
;


--query the date using the singlr field file format to understand the data 

select 
    METADATA$FILENAME::STRING as FILE_NAME
  , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
  , $1::VARIANT as CONTENTS
from @WEEK_1
  (file_format => 'FF_SINGLE_FIELD')
order by 
    FILE_NAME
  , ROW_NUMBER
;

----The contents field has a column header as result..no text qualifiers such as "" or (). makes it simple to load the date



--ingest the files into a table..creating FF_SINGLE_INGESTION since records end in \n...header is result we can skip that

create or replace file format FF_SINGLE_FIELD_INGESTION
    type = CSV
    field_delimiter = NONE
    record_delimiter = '\n'
    skip_header = 1
;

--creating a table to load the data from stage
create or replace table RAW_DATA (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , RESULT STRING
)
;


-- Ingesting the data in RAW_DATA table
COPY INTO RAW_DATA
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::string as CONTENTS
  from @WEEK_1
    (file_format => 'FF_SINGLE_FIELD_INGESTION')
)
;

--query the loaded data and see if all records are loaded
SELECT "RESULT" 
FROM RAW_DATA
ORDER BY 
    FILE_NAME
  , ROW_NUMBER
;

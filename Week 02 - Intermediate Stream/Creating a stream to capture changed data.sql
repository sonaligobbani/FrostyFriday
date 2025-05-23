use database SG_FROSTY_FRIDAY;


create or replace schema WEEK_2;

use schema WEEK_2;

--creating a stage 
create or replace stage STG_WEEK_2;

--listing the stage contents

list @STG_WEEK_2;

--creating a file format to load parquet data
CREATE OR REPLACE FILE FORMAT sf_tut_parquet_format
  TYPE = parquet;

-- This is empty to start, so we
-- need to upload our employees.parquet
-- file into this stage. This is achieved
-- with the following commands in a local
-- SnowSQL console:
/* --snowsql
snowsql -a my.account -u my_user -r my_role --private-key-path "path\to\my\ssh\key.p8"
PUT 'FILE://C:/My/Path/To/employees.parquet' @CH_FROSTY_FRIDAY.WEEK_2.STG_WEEK_2;
*/


list @STG_WEEK_2;

--Investigate the files using file format created

select 
    METADATA$FILENAME::STRING as FILE_NAME
  , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
  , $1::VARIANT as CONTENTS
from @STG_WEEK_2
  (file_format => 'sf_tut_parquet_format')
order by 
    FILE_NAME
  , ROW_NUMBER
;


-------------------------------
-- Ingest the files into a table

-- Create a table in which to land the data
create or replace table RAW_DATA (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , CONTENTS VARIANT
)
;

-- Ingest the data
COPY INTO RAW_DATA
FROM (
  select 
      METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::VARIANT as CONTENTS
  from @STG_WEEK_2
    (file_format => 'sf_tut_parquet_format')
)
;


--query the data from RAW_DATA
select CONTENTS
from RAW_DATA
order by FILE_NAME,ROW_NUMBER
;


-- Parse the raw data into another table

-- View the list of keys in the raw data
select distinct K.VALUE::string as OBJECT_KEYS
from RAW_DATA
  , table(flatten(OBJECT_KEYS(CONTENTS))) K 
;


-- Use the list of keys in the raw data
-- to create a parsed table. Also reordered
-- the columns a little to be tidy

create or replace table PARSED_DATA
as
select     
    CONTENTS:"employee_id"::string as employee_id
  , CONTENTS:"dept"::string as dept
  , CONTENTS:"job_title"::string as job_title
  , CONTENTS:"email"::string as email
  , CONTENTS:"title"::string as title
  , CONTENTS:"first_name"::string as first_name
  , CONTENTS:"last_name"::string as last_name
  , CONTENTS:"suffix"::string as suffix
  , CONTENTS:"education"::string as education
  , CONTENTS:"country_code"::string as country_code
  , CONTENTS:"country"::string as country
  , CONTENTS:"time_zone"::string as time_zone
  , CONTENTS:"city"::string as city
  , CONTENTS:"postcode"::string as postcode
  , CONTENTS:"street_name"::string as street_name
  , CONTENTS:"street_num"::string as street_num
  , CONTENTS:"payroll_iban"::string as payroll_iban
from RAW_DATA
;

-------------------------------
-- Create the stream on the parsed data table

-- Our objective is to have a stream that only
-- has dept and job_title
-- - dept
-- - job_title

-- To achieve this, we will create a view targeting
-- our desired fields (along with the employee ID)
-- and point our stream at that view

-- Create the view
create or replace view change_tracking
as
select
    employee_id
  , dept
  , job_title
from parsed_data
;


-- Create the stream

create or replace stream str_change_tracking
on view change_tracking
;

-- Query the stream to demonstrate
-- that it is empty to start
select * from str_change_tracking;


--Apply the changes from the given question 

UPDATE PARSED_DATA SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;
UPDATE PARSED_DATA SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;
UPDATE PARSED_DATA SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;
UPDATE PARSED_DATA SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;
UPDATE PARSED_DATA SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;


--query to see changes
select * from str_change_tracking;

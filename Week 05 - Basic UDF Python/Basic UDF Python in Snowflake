--WEEK5

------------------------------------------

--Creating a simple table with single column int
create or replace table SAMPLE_DATE
AS 
select
  uniform(1, 50, random())::int as START_INT
from (table(generator(rowcount => 50)));

---QUERY THE SAMPLE TABLE

select * from SAMPLE_DATE;

----creating a udf in python to multiply by 3

create or replace function multiply_by_three(i int)
returns int
language python
RUNTIME_VERSION = '3.9'
handler = 'multiply_by_three_py'
as
$$
def multiply_by_three_py(i):
  return i*3
$$;


---test the udf
select 
    START_INT
  , multiply_by_three(START_INT) as UDF_RESULT
  , START_INT*3 as VALIDATION_RESULT
  , UDF_RESULT = VALIDATION_RESULT as VALIDATION_FLAG
from SAMPLE_DATE
;





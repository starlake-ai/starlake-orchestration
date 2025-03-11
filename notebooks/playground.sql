USE DATABASE starlake1;
USE SCHEMA AUDIT;
CREATE TEMPORARY STAGE IF NOT EXISTS starlake_load_stage_YaVxLJyBbJ;
PUT file:///Users/hayssams/git/public/starlake/samples/spark/incoming/sales/categories-2018-01-01.csv @starlake_load_stage_YaVxLJyBbJ;
LIST @starlake_load_stage_YaVxLJyBbJ/;

create or replace table sales.categories2 (
    id number,
    name string,
    last_updated timestamp
);

starlake_load_stage_yavxljybbj/categories-2018-01-01.csv.gz;

COPY INTO sales.categories2
FROM @starlake_load_stage_YaVxLJyBbJ 
PATTERN='categories.*.csv.gz'
PURGE = TRUE
FILE_FORMAT = (TYPE = 'CSV' 
SKIP_HEADER = 1,
FIELD_DELIMITER = ',',
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    SKIP_BLANK_LINES = TRUE
);

REMOVE @starlake_load_stage_YaVxLJyBbJ/env PATTERN='.*.sl.yml.gz';

LIST @starlake_load_stage_YaVxLJyBbJ/;



drop stage starlake_load_stage_YaVxLJyBbJ;


SELECT * FROM sales.categories2;

truncate table sales.categories2;

select * from information_schema.columns ;

select column_name, data_type from information_schema.columns where table_schema ilike 'sales' and table_name ilike 'categories2';

show columns in  sales.categories2;


alter table sales.categories2 alter name set data type string; 
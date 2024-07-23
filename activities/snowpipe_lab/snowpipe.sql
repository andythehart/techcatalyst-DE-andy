
CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.andy
    STORAGE_INTEGRATION = s3_int
    URL='s3://techcatalyst-public/tatwan';

alter pipe TECHCATALYST_DE.Andy.ANDY_PIPE_1 refresh;

CREATE OR REPLACE STAGE TECHCATALYST_DE.Andy.Andy_AWS_STAGE
        --STORAGE_INTEGRATION = s3_int
        URL='s3://andy-techcatalyst-client/snowpipe/'
        CREDENTIALS = (AWS_KEY_ID= '', AWS_SECRET_KEY= ''); -- this code worked for snowpipe

LIST @TECHCATALYST_DE.EXTERNAL_STAGE.Andy PATTERN='.*csv.*';

LIST @TECHCATALYST_DE.andy.Andy_AWS_STAGE PATTERN='.*csv.*';


CREATE OR REPLACE FILE FORMAT andy_csv_format_stage
TYPE = 'CSV'
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1;


SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.Andy_AWS_STAGE/test',
    FILE_FORMAT=>'andy_csv_format_stage'
  )
);

SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.Andy/test',
    FILE_FORMAT=>'andy_csv_format_stage'
  )
);

drop table test;
create or replace transient table techcatalyst_de.andy.test
(
    name string,
    favnumber number
);

drop table test_1;
create or replace transient table techcatalyst_de.andy.test_1
(
    name string,
    favnumber number
);

--s3://andy-techcatalyst-client/test.csv

drop pipe techcatalyst_de.Andy.ANDY_PIPE_1;
create or replace pipe techcatalyst_de.Andy.ANDY_PIPE_1
auto_ingest = True
as 
copy into techcatalyst_de.andy.TEST_1
from  @TECHCATALYST_DE.Andy.Andy_AWS_STAGE
FILE_FORMAT = 'andy_csv_format_stage'; -- this code worked for snowpipe
--PATTERN= '.*test_1\\.csv$';


select * from test_1;




-- COPY INTO TECHCATALYST_DE.andy.TEST_2
-- FROM @TECHCATALYST_DE.Andy.Andy_AWS_STAGE
-- FILE_FORMAT = (FORMAT_NAME = andy_csv_format) PATTERN= '.*test\\.csv$';

COPY INTO TECHCATALYST_DE.andy.TEST
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.Andy
FILE_FORMAT = (FORMAT_NAME = andy_csv_format) PATTERN= '.*test\\.csv$';

-- create or replace pipe ANDY_PIPE_1
-- auto_ingest = True
-- as 
-- copy into techcatalyst_de.andy.test_1
-- from  @TECHCATALYST_DE.EXTERNAL_STAGE.Andy/tatwan/test.csv
-- FILE_FORMAT = 'andy_csv_format_stage'
-- --PATTERN= '.*test\\.csv$'
-- on_error = 'SKIP_FILE';


show pipes;


select * from information_schema.pipes;
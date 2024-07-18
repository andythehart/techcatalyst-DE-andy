USE SCHEMA techcatalyst_de.andy;
use schema techcatalyst_de.public;


drop stage TECHCATALYST_DE.EXTERNAL_STAGE.andy;
drop stage TECHCATALYST_DE.EXTERNAL_STAGE.andy_stocks;



CREATE OR REPLACE STAGE TECHCATALYST_DE.EXTERNAL_STAGE.andy
    STORAGE_INTEGRATION = s3_int
    URL='s3://techcatalyst-public/dw_stage/andy';



LIST @TECHCATALYST_DE.EXTERNAL_STAGE.andy;
LIST @TECHCATALYST_DE.EXTERNAL_STAGE.andy PATTERN='.*parquet.*';


SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.andy/songs_table',
    FILE_FORMAT=>'andy_stocks_parquet_format'
  )
);





SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION=>'@TECHCATALYST_DE.EXTERNAL_STAGE.andy/songplays_table',
    FILE_FORMAT=>'andy_stocks_parquet_format'
  )
);









create or replace TRANSIENT TABLE TECHCATALYST_DE.andy.USER_DIM (

    id varchar(16777216),
    firstname varchar (16777216),
    lastname varchar (16777216),
    gender varchar (10),
    level varchar(16777216)
    
);






COPY INTO TECHCATALYST_DE.andy.USER_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/user_table
FILE_FORMAT = 'andy_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;






create or replace TRANSIENT TABLE TECHCATALYST_DE.andy.SONGS_DIM (
    
    song_id varchar(16777216),
    title    varchar(16777216),
    duration float

);


COPY INTO TECHCATALYST_DE.andy.SONGS_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/songs_table
FILE_FORMAT = 'andy_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select * from songs_dim;


create or replace TRANSIENT TABLE TECHCATALYST_DE.andy.TIME_DIM (

    ts number(38,0),
    datetime timestamp,
    start_time varchar(16777216),
    dayofmonth number(38,0),
    weekofyear number(38,0)
    

);



COPY INTO TECHCATALYST_DE.andy.TIME_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/time_table
FILE_FORMAT = 'andy_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;

select * from TIME_DIM;


create  or replace TRANSIENT TABLE TECHCATALYST_DE.andy.ARTIST_DIM (

    artist_id varchar(16777216),
    artist_name varchar (16777216),
    artist_location varchar (16777216),
    artist_latitude float,
    artist_longitude float
);

COPY INTO TECHCATALYST_DE.andy.ARTIST_DIM
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/artists_table
FILE_FORMAT = 'andy_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


create or replace TRANSIENT TABLE TECHCATALYST_DE.andy.SONGPLAYS_FACT (

    id varchar(16777216),
    datetime_id varchar(16777216),
    level varchar(16777216),
    song_id varchar(16777216),
    artist_id varchar(16777216),
    sessionId varchar(16777216),
    location varchar(16777216),
    useragent varchar(16777216)

);

COPY INTO TECHCATALYST_DE.andy.SONGPLAYS_FACT
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/songplays_table
FILE_FORMAT = 'andy_parquet_format'
ON_ERROR = CONTINUE
MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;


select * from SONGPLAYS_FACT;



CREATE OR REPLACE TRANSIENT TABLE TEMP_SONGS_DIM (
    SONG_ID STRING,
    TITLE STRING,
    YEAR STRING,
    ARTIST_ID STRING,
    DURATION FLOAT,
    PARTITION_YEAR STRING,
    PARTITION_ARTIST_ID STRING
);


INSERT INTO TEMP_SONGS_DIM (SONG_ID, TITLE, YEAR, ARTIST_ID, DURATION, PARTITION_YEAR, PARTITION_ARTIST_ID)
SELECT
    $1:song_id::STRING AS SONG_ID,
    $1:title::STRING AS TITLE,
    $1:year::STRING AS YEAR,
    $1:artist_id::STRING AS ARTIST_ID,
    $1:duration::FLOAT AS DURATION,
    REGEXP_SUBSTR(METADATA$FILENAME, 'year=(\\d+)', 1, 1, 'e')::STRING AS PARTITION_YEAR,
    REGEXP_SUBSTR(METADATA$FILENAME, 'artist_id=([^/]+)', 1, 1, 'e')::STRING AS PARTITION_ARTIST_ID
FROM @TECHCATALYST_DE.EXTERNAL_STAGE.andy/songs_table(FILE_FORMAT => 'andy_parquet_format', PATTERN => '.*parquet.*');




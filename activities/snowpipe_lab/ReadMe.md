# Snowflake Process Documentation

This folder contains documentation for understanding and implementing the Snowflake process. The Snowflake process is a method used in data warehousing to transform raw data into a format suitable for analysis. It involves three main stages: loading, transforming, and analyzing data. Files where add by using s3.upload_file from a local machine. As Bouns lambda function was implemented to turn all csv files into parquet and uploaded to an another s3 bucket.

### Also check tarak repo for aws configuration

## Overview

The Snowflake process is designed to efficiently manage large volumes of data while ensuring data quality and accessibility for analytical queries. It consists of:

1. **Loading**: Ingesting raw data from various sources into a staging area.
2. **Transforming**: Cleaning, aggregating, and structuring the data into a format suitable for analysis.
3. **Analyzing**: Querying the transformed data to extract insights and make informed decisions.

![Snowflake Process Diagram](/activities/snowpipe_lab/images/last_and_final.JPG)


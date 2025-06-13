-- Create the COVID_ANALYTICS table if it doesn't exist
CREATE TABLE IF NOT EXISTS ETL_PROJECT.PUBLIC.COVID_ANALYTICS (
    State VARCHAR,
    Year INT,
    Month INT,
    Total_Indian BIGINT,
    Total_Foreigners BIGINT,
    Total_Cured BIGINT,
    Total_Deaths BIGINT
);

-- Create or replace the storage integration
CREATE OR REPLACE STORAGE INTEGRATION s3_covid_integration
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::xxxxxxxxx:role/GlueETLrole'
STORAGE_ALLOWED_LOCATIONS = ('s3://snowflakecoviddata-asb/transformed_covid/');

-- Describe the integration to get IAM user ARN and external ID
DESC INTEGRATION s3_covid_integration;

-- Create or replace the stage
CREATE OR REPLACE STAGE ETL_PROJECT.PUBLIC.COVID_TRANSFORMED_STAGE
STORAGE_INTEGRATION = s3_covid_integration
URL = 's3://snowflakecoviddata-asb/transformed_covid/'
FILE_FORMAT = (TYPE = PARQUET);

-- List files in the stage to verify access
LIST @ETL_PROJECT.PUBLIC.COVID_TRANSFORMED_STAGE;

-- Load data into the table
COPY INTO ETL_PROJECT.PUBLIC.COVID_ANALYTICS
FROM @ETL_PROJECT.PUBLIC.COVID_TRANSFORMED_STAGE
FILE_FORMAT = (TYPE = PARQUET MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE)
ON_ERROR = 'CONTINUE';

-- Verify the loaded data
SELECT * FROM ETL_PROJECT.PUBLIC.COVID_ANALYTICS LIMIT 10;

-- Grant usage on integration (if needed, run as admin)
GRANT USAGE ON INTEGRATION s3_covid_integration TO ROLE YOUR_ROLE;
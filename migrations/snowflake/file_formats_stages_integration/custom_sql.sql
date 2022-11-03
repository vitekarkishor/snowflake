USE DATABASE DEMO_NEW_DB;

-- FILE FORMAT PARQUET
CREATE FILE FORMAT COM.PARQUET_FF
TYPE = 'PARQUET' 
COMPRESSION = 'AUTO' 
BINARY_AS_TEXT = FALSE
COMMENT = 'PARQUET FILE FORMAT TO LOAD THE FILES IN S3 WITH DEFAULT SNAPPY COMPRESSION';

-- FILE FORMAT ORC
CREATE FILE FORMAT COM.ORC_FF
TYPE = 'ORC' 
COMMENT = 'ORC FILE FORMAT TO LOAD THE FILES IN S3 WITH DEFAULT COMPRESSION';

-- STORAGE INTEGRATION
--CREATE STORAGE INTEGRATION S3_INT
  --TYPE = EXTERNAL_STAGE
  --STORAGE_PROVIDER = 'S3'
  --STORAGE_AWS_ROLE_ARN = 'ARN:AWS:IAM::771144769606:ROLE/SNOWFLAKE_ROLE'
  --ENABLED = TRUE
  --STORAGE_ALLOWED_LOCATIONS = ('S3://DEVOPS-RAW-KISHOR/RAW/');
  
-- EXTERNAL STAGE FOR PARQUET  
CREATE OR REPLACE STAGE COM.EXT_STAGE_RAW_PARQUET
STORAGE_INTEGRATION = S3_INT
URL = 's3://devops-raw-kishor/raw/'  
FILE_FORMAT =(FORMAT_NAME = COM.PARQUET_FF) 
COMMENT = 'STAGE TO LOAD THE PARQUET DATA FROM S3 BUCKET';

-- EXTERNAL STAGE FOR ORC 
CREATE OR REPLACE STAGE COM.EXT_STAGE_RAW_ORC
STORAGE_INTEGRATION = S3_INT
URL = 's3://devops-raw-kishor/raw/'  
FILE_FORMAT =(FORMAT_NAME = COM.ORC_FF) 
COMMENT = 'STAGE TO LOAD THE ORC DATA FROM S3 BUCKET';
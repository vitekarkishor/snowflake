Snowflake Manual Deployment Flow
1.	Schema’s
	Path: snowflake\schemas\SCHEMAS.sql
	Example SQL:
		CREATE SCHEMA IF NOT EXISTS Z1_STG_FDM;
		CREATE SCHEMA IF NOT EXISTS Z2_CORE_FDM;
		CREATE SCHEMA IF NOT EXISTS Z3_ACS_FDM;
		CREATE SCHEMA IF NOT EXISTS COM;

2.	File Formats
	Path: snowflake\stages\generic\EXT_STAGES_DDL.sql
	Example SQL:
		PARQUET
			CREATE FILE FORMAT COM.PARQUET_FF
			TYPE = 'PARQUET' 
			COMPRESSION = 'AUTO' 
			BINARY_AS_TEXT = FALSE
			COMMENT = 'PARQUET FILE FORMAT TO LOAD THE FILES IN S3 WITH DEFAULT SNAPPY COMPRESSION';		 
		ORC
			CREATE FILE FORMAT COM.ORC_FF
			TYPE = 'ORC' 
			COMMENT = 'ORC FILE FORMAT TO LOAD THE FILES IN S3 WITH DEFAULT COMPRESSION';

3.	Stages 
	Path: snowflake\stages\generic\EXT_STAGES_DDL.sql
	Example SQL:
		PARQUET -
			CREATE OR REPLACE STAGE COM.EXT_STAGE_RDB_PARQUET
			STORAGE_INTEGRATION = S3_PROD_RDB
			URL = 's3://s3-prd-edo-data-olyp/s2/data/rdb'  
			FILE_FORMAT =(FORMAT_NAME = COM.PARQUET_FF) 
			COMMENT = 'STAGE TO LOAD THE PARQUET DATA FROM S3 BUCKET';
		ORC -
			CREATE OR REPLACE STAGE COM.EXT_STAGE_RDB_ORC
			STORAGE_INTEGRATION = S3_PROD_RDB
			URL = 's3://s3-prd-edo-data-olyp/s2/data/rdb'  
			FILE_FORMAT =(FORMAT_NAME = COM.ORC_FF) 
			COMMENT = 'STAGE TO LOAD THE ORC DATA FROM S3 BUCKET';
			
4.	Tables
	RAW -
		Path: snowflake\table\raw\Z1_STG_FDM_RAW.sql
		Example SQL:
			create or replace TRANSIENT TABLE Z1_STG_FDM.DUMMY_RAW (
			RAW VARIANT);

	STG -
		Path: snowflake\table\stg\Z1_STG_FDM_STG.sql
		Example SQL:
			create or replace TRANSIENT TABLE Z1_STG_FDM.DUMMY_STG (
				ADJUSTMENT_ID NUMBER(38,0) NOT NULL,
				PRIMARY_SUBSCRIBER_ID NUMBER(38,0));

	TARGET -
		Path: snowflake\table\prod\Z2_CORE_FDM_TARGET.sql
		Example SQL:
			create or replace TABLE Z2_CORE_FDM.DUMMY (
				ADJUSTMENT_ID NUMBER(38,0) NOT NULL,
				PRIMARY_SUBSCRIBER_ID NUMBER(38,0));

	COM -
		Path: snowflake\table\com\COM_LOGS.sql
		List of tables
			•	DQ_ERROR_TABLE
			•	DQ_MAPPING_TABLE
			•	MASTER_AUDIT_LOG
			•	PROCESS_AUDIT_LOG
			•	SAVE_COPY_ERROR
			•	SCD_MAPPING_TABLE
			•	TEST_RESULT

5.	Views
	On RAW Table - 
		Path: snowflake\views\Z1_STG_FDM _VW.sql
		Example SQL:
			create or replace view DEV_DB_FDM.Z1_STG_FDM.DUMMY_VW( 
			ADJUSTMENT_ID, 
			RIMARY_SUBSCRIBER_ID 
			) as SELECT 
			$1 : adjustment_id :: varchar as adjustment_id,  
			$1 : primary_subscriber_id :: varchar as primary_subscriber_id 
			FROM Z1_STG_FDM.DUMMY_RAW;
		
	On Target Table -
		Path: snowflake\views\Z3_ACS_FDM _VW.sql
		Example SQL:
			create or replace view DEV_DB_FDM. Z3_ACS_FDM.DUMMY_VW(
			ADJUSTMENT_ID,
			PRIMARY_SUBSCRIBER_ID
			) as SELECT *  FROM Z2_CORE_FDM.DUMMY;

	On COM -
		Path: snowflake\views\COM_LOGS_VW.sql
		Example SQL:
			•	COPY_PROCESS_LOG_VW
			•	DML_PROCESS_LOG_VW
			•	DQ_ERROR_TABLE_VW
			•	MASTER_LOG_VW
			•	SAVE_COPY_ERROR_VW

6.	Create Warehouse if needed (Optional)
	CREATE OR REPLACE WAREHOUSE my_wh W ITH WAREHOUSE_SIZE='X-LARGE';

7.	Common Functions
	Path: snowflake\frameworks\dq_frameworks\functions\dq_functions.sql
		Deploy common functions
			•	not_null_chk_ud
			•	length_chk_ud

8.	Frameworks
	Path: snowflake\frameworks\*
	Deploy below frameworks
		•	delete_insert_framework
		•	dq_frameworks
		•	loading_framework
		•	scd_framework
		•	testing_framework
		•	truncate_framework 

9.	Individual SP’s
	Deploy Individual SP’s
		•	GENERIC_FETCH_DDLS_SP
		•	CREATE_SEMISTRUCTURED_VIEW_SCHEMA_DETECTION_SP
		•	GENERIC_FETCH_DDLS_SP 

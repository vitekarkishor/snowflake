create or replace TABLE COM.PROCESS_AUDIT_LOG (
	EXECUTION_ID VARCHAR(16777216),
	ETL_NAME VARCHAR(16777216),
	ETL_TASK_NAME VARCHAR(16777216),
	EXECUTED_SP VARCHAR(16777216),
	START_TIME TIMESTAMP_NTZ(9),
	END_TIME TIMESTAMP_NTZ(9),
	STATUS VARCHAR(16777216),
	DETAILS_AFTER_EXEC VARIANT
);
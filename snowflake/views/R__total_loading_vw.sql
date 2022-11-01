create or replace view COM.TOTAL_LOADING_VW(
	EXECUTION_ID,
	RAW_TABLE_NAME,
	EXECUTION_DATE,
	RAW_PARTITION_LOADED,
	RAW_ROWS_LOADED,
	RAW_NO_OF_FILES_LOADED,
	TARGET_ROWS_LOADED,
	TARGET_ROWS_UPDATED,
	DQ_ERROR_RECORDS
) as ( 
    select 
        loading_vw.execution_id, 
        loading_vw.raw_table_name,
        load_date as execution_date,
        loading_vw.raw_partition_loaded,
        loading_vw.raw_rows_loaded, 
        loading_vw.raw_no_of_files_loaded,
        dml_vw.target_rows_loaded, 
        dml_vw.target_rows_updated,
        dq_error_vw.DQ_ERROR_RECORDS
        from COM.LOADING_AGG_VW loading_vw
        left join COM.DML_AGG_VW dml_vw
        on loading_vw.execution_id = dml_vw.execution_id
        and loading_vw.raw_table_name = dml_vw.target_table_name
        left join COM.DQ_ERROR_AGG_VW dq_error_vw
        on loading_vw.execution_id = dq_error_vw.execution_id
        and loading_vw.raw_table_name = dq_error_vw.dq_table_name
);
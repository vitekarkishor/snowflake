create or replace view COM.LOADING_AGG_VW(
	EXECUTION_ID,
	RAW_TABLE_NAME,
	LOAD_DATE,
	RAW_PARTITION_LOADED,
	RAW_ROWS_LOADED,
	RAW_NO_OF_FILES_LOADED
) as (
    select
        raw.execution_id as execution_id,
        replace(split_part(raw.table_name,'.',2),'_RAW','') as raw_table_name,
        to_date(raw.load_start_time) as load_date,
        min(to_date(raw.partition_date)) as raw_partition_loaded,
        sum(raw.rows_loaded) as raw_rows_loaded,
        count(raw.file_name) as raw_no_of_files_loaded
        from COM.SF_LOADING_STATS_VW raw
        group by 1,2,3
);
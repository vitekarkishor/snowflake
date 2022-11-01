create or replace view COM.DML_AGG_VW(
	TARGET_TABLE_NAME,
	TARGET_ROWS_LOADED,
	TARGET_ROWS_UPDATED,
	EXECUTION_ID
) as (
  select
    split_part(target.destination,'.',2) as target_table_name,
    sum(target.rows_inserted) as target_rows_loaded,
    sum(target.rows_updated) as target_rows_updated,
    target.execution_id as execution_id
    from "ADT"."DML_PROCESS_LOG_VW" target
    group by execution_id,target_table_name
);
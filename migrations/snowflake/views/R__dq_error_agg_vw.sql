create view COM.DQ_ERROR_AGG_VW as (
	select
        execution_id,
        replace(table_name,'_VW','') as dq_table_name,
        count(1) as DQ_ERROR_RECORDS
        from COM.DQ_ERROR_TABLE
        group by
        execution_id,dq_table_name
);
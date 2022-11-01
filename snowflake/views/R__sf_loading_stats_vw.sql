create or replace view COM.SF_LOADING_STATS_VW(
	EXECUTION_ID,
	TABLE_NAME,
	LOAD_START_TIME,
	FILE_NAME,
	PARTITION_FORMAT,
	PARTITION_DATE,
	ROWS_LOADED,
	COPY_TIME,
	LOAD_STATUS,
	ERROR_MESSAGE
) as
select
EXECUTION_ID,
table_name,
load_start_time,
file_name,
split_part(partition_value_loaded,'=',1) as partition_format,
split_part(partition_value_loaded,'=',2) as partition_date,
rows_loaded,
copy_time,
load_status,
error_message
from 
(
select 
EXECUTION_ID,
destination as table_name,
  start_time as load_start_time,
  file_name,
    split_part(file_name,'/',6) as src_system,
  case
    when contains(file_name,'pxn_hr') and src_system in ('network_datamart','broadband_datamart')
    then 'pxn_yr/pxn_mo/pxn_dy/pxn_hr='||(split_part(split_part(file_name,'/',8),'=',2)||'-'||split_part(split_part(file_name,'/',9),'=',2)||'-'||split_part(split_part(file_name,'/',10),'=',2)||' '||split_part(split_part(file_name,'/',11),'=',2)||':00:00')
    when contains(file_name,'pxn_mo') and src_system in ('rdb')
    then 'pxn_yr/pxn_mo='||(split_part(split_part(file_name,'/',9),'=',2)||'-'||split_part(split_part(file_name,'/',10),'=',2)||'-'||'01')
    when contains(file_name,'pxn_mo') and src_system in ('broadband_datamart')
    then 'pxn_yr/pxn_mo/pxn_dy='||(split_part(split_part(file_name,'/',8),'=',2)||'-'||split_part(split_part(file_name,'/',9),'=',2)||'-'||split_part(split_part(file_name,'/',10),'=',2))
    when src_system in ('rdb')
    then split_part(file_name,'/',9)
    when src_system in ('network_datamart','broadband_datamart')
    then split_part(file_name,'/',8)
	when contains(file_name,'pxn_mo') and src_system in ('padawan')
    then 'pxn_yr/pxn_mo/pxn_dy='||(split_part(split_part(file_name,'/',12),'=',2)||'-'||split_part(split_part(file_name,'/',13),'=',2)||'-'||split_part(split_part(file_name,'/',14),'=',2))
    else NULL
  end as partition_value_loaded,
rows_loaded,
timediff(seconds,start_time,end_time) as time,
case when time < 60 then time||' seconds' else time/60 || ' minutes' end as copy_time,
load_status,
error_message
from COM.COPY_PROCESS_LOG_VW
where 
query_type is not null)
order by table_name,partition_date desc
;
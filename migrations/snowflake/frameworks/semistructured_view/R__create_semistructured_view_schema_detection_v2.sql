CREATE OR REPLACE PROCEDURE COM.CREATE_SEMISTRUCTURED_VIEW_SCHEMA_DETECTION_V2("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

//---common function to execute the statement
function executeStatement(exec_type, query_cmd, binding_vars) {


if (exec_type == 'binding_vars') {
var query_stmt = snowflake.createStatement(
{
sqlText: query_cmd
, binds: binding_vars
}
);
}
else {
var query_stmt = snowflake.createStatement(
{
sqlText: query_cmd
}
);
}
var res_out = query_stmt.execute();
var query_id = query_stmt.getQueryId();
return { res_out, query_id };
}



var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
var database_name = constraints_list_json["database_name"];
var schema_name = constraints_list_json["schema_name"];
var table_name = constraints_list_json["table_name"];
var file_prefix_path = constraints_list_json["file_prefix_path"];
var file_format = constraints_list_json["file_format"];
var uploaded_date = constraints_list_json["uploaded_date"];
var load_date = constraints_list_json["load_date"];
var etl_name = constraints_list_json["etl_name"].toUpperCase();
var etl_task_name = constraints_list_json["etl_task_name"].toUpperCase();
var stage_name = constraints_list_json["stage_name"].toUpperCase();


//------ Get columns from S3 using infer_schema
var query_cmd = "select distinct concat('$1:',column_name,'::varchar as ',column_name) from table (INFER_SCHEMA(LOCATION => '@COM."+stage_name+"/" + file_prefix_path + "', FILE_FORMAT => '" + file_format + "'));"
var select_out = executeStatement('binding_vars', query_cmd, [file_prefix_path,file_format]);
var resOut = select_out.res_out;
resOut.next()



// Create array of all the columns and then convert into , seprated list
arr = []
arr.push(resOut.getColumnValue(1))
while (resOut.next()) {
arr.push(resOut.getColumnValue(1)); 
}
var newvar = arr.join(',')



//------View creation ddl and execution
var view_ddl = "CREATE OR REPLACE VIEW " + database_name+"."+schema_name+"."+table_name+"_VW" + " AS SELECT " + newvar+ " FROM "+ database_name+"."+schema_name+"."+table_name+"_RAW";
var select_out = executeStatement('binding_vars', view_ddl, [database_name,schema_name,table_name]);
var resOut = select_out.res_out;
resOut.next()
return resOut.getColumnValue(1)
$$;
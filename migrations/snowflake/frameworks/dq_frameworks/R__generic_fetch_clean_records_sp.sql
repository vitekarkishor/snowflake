CREATE OR REPLACE PROCEDURE COM.GENERIC_FETCH_CLEAN_RECORDS_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

try
{
    function executeStatement(exec_type, query_cmd, binding_vars){
        if (exec_type == 'binding_vars') {
            var query_stmt = snowflake.createStatement(
            {
                sqlText: query_cmd
                , binds: binding_vars
            }
            );
        }
        else{
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

    function call_common_function(function_name,converted_args_json,default_args_json){
    var mstr_call_cmd = 'CALL COM.COMMON_FUNCTIONS_SP(:1,:2,:3) ;';
    var binding_vars = [function_name, converted_args_json, default_args_json]
    var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
    var resOut = exec_res_out.res_out;
    resOut.next();
    var funcResOut = resOut.getColumnValueAsString(1);
    return funcResOut;
    }

    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var etl_name = constraint_list_json["etl_name"].toUpperCase();
    var etl_task_name = constraint_list_json["etl_task_name"].toUpperCase();
    var source_database_name = constraint_list_json["source_database_name"].toUpperCase();
    var source_schema_name = constraint_list_json["source_schema_name"].toUpperCase();
    var source_table_name = constraint_list_json["source_table_name"].toUpperCase();
    var target_database_name = constraint_list_json["target_database_name"].toUpperCase();
    var target_schema_name = constraint_list_json["target_schema_name"].toUpperCase();
    var target_table_name = constraint_list_json["target_table_name"].toUpperCase();
    var error_schema_name = constraint_list_json["error_schema_name"].toUpperCase();
    var error_table_name = constraint_list_json["error_table_name"].toUpperCase();
    var executed_sp = constraint_list_json["executed_sp"].toUpperCase();
    var execution_id = constraint_list_json["execution_id"];
    var uploaded_date = constraint_list_json["uploaded_date"];
    var load_date = constraint_list_json["load_date"];

    extra_info = JSON.stringify({"status":"Started", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

    snowflake.execute(
    {
    sqlText: `BEGIN TRANSACTION`
    }
    );

    // CODE FOR GETTING COLUMN NAMES

    var select_cmd = `SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = :1
                    AND TABLE_SCHEMA= :2
                    AND TABLE_NAME = :3
                    ;`;

    var select_out = executeStatement('binding_vars', select_cmd, [target_database_name, target_schema_name, target_table_name])

    var resOut = select_out.res_out;
    var select_out_array = [];

    while (resOut.next()) {
        var col_name = resOut.COLUMN_NAME;
        select_out_array.push(col_name);
    }

    var table_columns_name = select_out_array.join(",");
    
    // CODE FOR GETTING IDENTIFIER NAMES

    var select_cmd = 'SELECT DISTINCT IDENTIFIER_NAME ' + 'FROM COM.DQ_MAPPING_TABLE WHERE DATABASE_NAME = ' +"'"+ source_database_name +"'"+ ' AND SCHEMA_NAME = ' +"'"+ source_schema_name +"'"+ ' AND TABLE_NAME = ' +"'"+ source_table_name +"'"+ ';';

    var select_out = executeStatement('binding_vars', select_cmd, [source_database_name, source_schema_name, source_table_name]);

    var resOut = select_out.res_out;
    resOut.next();

    var identifier_name = resOut.IDENTIFIER_NAME;
    var error_info_identifier_name = identifier_name.split(',');
    var i = 0;
    error_info_array = [];
    var not_null_condition = "";

    while(i < error_info_identifier_name.length){
      var key_value = 'ERROR_INFO:' + error_info_identifier_name[i];
      error_info_array.push(key_value);
      not_null_condition = not_null_condition + " AND " + key_value + " IS NOT NULL "
      i++;
    }
    var error_info = error_info_array.join(',');

    //------truncate table if truncate_load is set to yes
    if (constraint_list_json.hasOwnProperty("truncate_load") && constraint_list_json["truncate_load"].toUpperCase() == 'YES' ){
        var truncate_cmd = `truncate table `+ target_database_name + `.` + target_schema_name + `.` + target_table_name + `;`
        executeStatement('', truncate_cmd, []);
    }

    var sql_stmt = 'INSERT INTO ' + target_database_name + '.' + target_schema_name + '.' + target_table_name + '(' + table_columns_name + ')' +
                        ' SELECT ' + table_columns_name + ' FROM ' + source_database_name + '.' + source_schema_name + '.' + source_table_name + 
                        ' WHERE (' + identifier_name + ') NOT IN (SELECT ' + error_info + ' FROM ' + source_database_name + '.' + error_schema_name + '.' + error_table_name + 
                        ' WHERE DATABASE_NAME = ' +"'"+ source_database_name +"'"+ ' AND SCHEMA_NAME = ' +"'"+ source_schema_name +"'"+ ' AND TABLE_NAME = ' +"'"+ source_table_name +"'"+ ' AND EXECUTION_ID = ' +"'"+ execution_id +"'"+ not_null_condition + ');';

    var exec_res_out = executeStatement('', sql_stmt, []);
    var query_id = exec_res_out.query_id;

    var process_log_input_params = `{"query_id":"` + query_id + `","query_type":"INSERT", "status" : "Success" , "destination" : "`+target_schema_name+`.`+target_table_name+`", "start_time": "`+ START_TIME + `"}`
    var dtls_after_exec = call_common_function('update_process_audit_log' , process_log_input_params, CONSTRAINTS_LIST);

    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

    snowflake.execute({ sqlText: `COMMIT` });

    var resOut = exec_res_out.res_out;
    resOut.next();
    var RowCount = resOut.getColumnValueAsString(1);

    return 'Success -> Clean records inserted - ' + RowCount;

}
catch (err)
{

    var last_query_cmd = `select last_query_id()`;
    var exec_res_out = executeStatement('', last_query_cmd, []);
    var last_query_res = exec_res_out.res_out;
    last_query_res.next();
    var error_query_id = last_query_res.getColumnValue(1);

    var message = err.message.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
	var stack_trace_txt = "";
	if(err.stackTraceTxt) {
		stack_trace_txt = err.stackTraceTxt.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
	}
    var dtls_after_exec = JSON.stringify(
    {
    "FAIL_CODE": err.code,
    "STATE": err.state,
    "MESSAGE": message,
    "STACK_TRACE": stack_trace_txt,
    "QUERY_ID": error_query_id,
    "EXECUTED_SP" : executed_sp,
    "EXECUTION_STATUS": "FAILURE"
    }
    );

    snowflake.execute({ sqlText: `ROLLBACK` });

    var common_fun_param = `{"execution_id":"` + EXECUTION_ID +
    `","etl_name":"` + etl_name +
    `","etl_task_name":"` + etl_task_name +
    `","executed_sp":"` + executed_sp +
    `","start_time":"` + START_TIME +
    `","status":"Failure"
    }`;

    call_common_function('insert_into_process_audit_log' , common_fun_param, dtls_after_exec);

    //-----MAKE FAILED ENTRY IN THE MASTER AUDIT LOG

    extra_info = JSON.stringify({"status":"Failed", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
}

$$;
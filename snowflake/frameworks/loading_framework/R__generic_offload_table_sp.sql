CREATE OR REPLACE PROCEDURE COM.GENERIC_OFFLOAD_TABLE_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {
//----------AS IS CODE----BEGIN--------------------------------------------------------------------

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
	
	
	function call_common_function(function_name,converted_args_json,default_args_json){
	    var mstr_call_cmd = 'CALL COM.COMMON_FUNCTIONS_SP(:1,:2,:3) ;';
        var binding_vars = [function_name, converted_args_json, default_args_json]
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;
        
	}
    

    //----define some custome variables
    var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
    var etl_name = constraints_list_json["etl_name"].toUpperCase();
    var etl_task_name = constraints_list_json["etl_task_name"].toUpperCase();
    var executed_sp = constraints_list_json["executed_sp"].toUpperCase();
    var uploaded_date = constraints_list_json["uploaded_date"];
    var load_date = constraints_list_json["load_date"];

    //-----make started entry in the master audit log
    extra_info = JSON.stringify({"status":"Started", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
	
    snowflake.execute(
        {
            sqlText: `BEGIN TRANSACTION`
        }
    );


//----------AS IS CODE------END---------------------------------------------------------------

    //----------Begin of Your Functional Req code--------------------------
    
     var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
    var database_name = constraints_list_json["database_name"];
    var schema_name = constraints_list_json["schema_name"];
    var stage_name = constraints_list_json["stage_name"];
    var table_name = constraints_list_json["table_name"];
    var folder_name = constraints_list_json["folder_name"];
    var query_columns = constraints_list_json["query_columns"];
    var condition_value = constraints_list_json["condition"];
    var limit_value = constraints_list_json ["limit_value"];
    //var date_format = constraints_list_json ["date_format"];
    var executed_sp = constraints_list_json["executed_sp"];
    var execution_id = constraints_list_json["execution_id"];

	var today = new Date();
	var date = today.getFullYear()+''+(today.getMonth()+1)+''+today.getDate();
	var time = today.getHours() + "" + today.getMinutes() + "" + today.getSeconds();
	var dateTime = date+time;
	
    var target_dir = stage_name + `/`+ folder_name + `/`+dateTime
   
    if (condition_value == undefined || condition_value.length == 0 ){
       var condition_cmd = ` `;
    } else {
        var condition_cmd = ` Where EXECUTION_ID = '` + execution_id + `' ` ;
    }
   
    if (query_columns == undefined || query_columns.length == 0 || query_columns == 0 ){
        var columns_cmd = ` * `;
    } else {
        var columns_cmd = ` ` + query_columns +` ` ;
    }
   
    if (limit_value == undefined || limit_value == 0 || limit_value.length == 0 ){
       var limit_cmd = ` `;
    } else {
        var limit_cmd = ` LIMIT ` + limit_value + ` ` ;
    }
   
    //if (date_format != undefined && date_format.length != 0 ){
        //var alter_date_op_format = `ALTER SESSION SET DATE_OUTPUT_FORMAT = '` + date_format + `';`;
       // executeStatement('', alter_date_op_format, [])}
     
    var copy_cmd = `COPY INTO @` + target_dir
                    + ` FROM ( select ` + columns_cmd  + ` from `
                    + database_name + `.` + schema_name + `.` + table_name
                    + condition_cmd
                    + limit_cmd + ` ) OVERWRITE = TRUE, header = true;`
   

    //---execute the copy command
    var exec_res_out = executeStatement('', copy_cmd, []);
    var query_id = exec_res_out.query_id;
    

	
    //--get the copy command query execution history
	var common_fun_param = `{"query_id":"` + query_id + `","query_type":"UNLOAD"}`
	var dtls_after_exec = call_common_function('get_query_history_result' , common_fun_param, '');

    dtls_after_exec = JSON.parse(dtls_after_exec) 

    var success_unloaded_row_count = 0;
    var error_unloaded_row_count = 0;

    for (var col_num = 0; col_num < dtls_after_exec.length; col_num = col_num + 1) {
        var dtls_after_exec_json = dtls_after_exec[col_num]
        var load_status = dtls_after_exec_json["LOAD_STATUS"];
        var rows_loaded = dtls_after_exec_json["ROWS_LOADED"];
        dtls_after_exec_json["EXECUTED_SP"] = executed_sp;
        dtls_after_exec_json["SOURCE"] = database_name + `.` + schema_name + `.` + table_name;
        dtls_after_exec_json["DESTINATION"] = `S3: `+ target_dir;
        if (load_status == 'UNLOADED') {
            success_unloaded_row_count = success_unloaded_row_count + rows_loaded;
        }
        else {
            error_unloaded_row_count = error_unloaded_row_count + rows_loaded;
        }
    }

    dtls_after_exec["EXECUTED_SP"] = executed_sp;    
    dtls_after_exec["DESTINATION"] = 'S3';
    dtls_after_exec["UPLOADED_DATE"] = uploaded_date;    
    dtls_after_exec["LOAD_DATE"] = load_date;
               
    var common_fun_param = `{"execution_id":"` + EXECUTION_ID + 
                            `","etl_name":"` + etl_name + 
                            `","etl_task_name":"` + etl_task_name + 
                            `","executed_sp":"` + executed_sp + 
                            `","start_time":"` + START_TIME +
                            `","status":"Success"
                            }`;
    
    dtls_after_exec = JSON.stringify(dtls_after_exec);
	call_common_function('insert_into_process_audit_log' , common_fun_param, dtls_after_exec);

    //-----make Completion entry in the master audit log
    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info); 	
	
    snowflake.execute({ sqlText: `COMMIT` });
    
    return 'Success -> Number of rows unloaded: ' + success_unloaded_row_count + ' and Number of rows not unloaded(error): ' + error_unloaded_row_count ;
}
catch (err) {
    
    var last_query_cmd =  `select last_query_id()`;
    var exec_res_out = executeStatement('', last_query_cmd, []);
    var last_query_res = exec_res_out.res_out;
    last_query_res.next();
    var error_query_id = last_query_res.getColumnValue(1);

    var message = err.message.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
    var dtls_after_exec = JSON.stringify(
        {
            "FAIL_CODE": err.code,
            "STATE": err.state,
            "MESSAGE": message,
            "STACK_TRACE": err.stackTraceTxt,
            "QUERY_ID":error_query_id,
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

    //-----make Failed entry in the master audit log
    extra_info = JSON.stringify({"status":"Failed", "start_time": START_TIME});
	call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
	
    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
//----------AS IS CODE----END--------------------------------------------------------------------
}
$$;
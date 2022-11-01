CREATE OR REPLACE PROCEDURE COM.GENERIC_LOAD_STG_TABLE_CASE_SENSITIVE_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {

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

	//-------function to call common functions
	function call_common_function(function_name,converted_args_json,default_args_json){
	    var mstr_call_cmd = 'CALL COM.COMMON_FUNCTIONS_SP(:1,:2,:3) ;';
        var binding_vars = [function_name, converted_args_json, default_args_json];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;

	}

	function call_off_load_sp(execution_id,start_time,constraints_list){
	    var mstr_call_cmd = 'CALL COM.GENERIC_OFFLOAD_TABLE_SP(:1,:2,:3) ;';
        var binding_vars = [execution_id, start_time, constraints_list]
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd,binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;

	}
    //------function to truncate table if truncate_load is set to yes
    function truncate_table_before_load() {
		var common_fun_param = `{"json_key":"truncate_load","default_value":"no"}`;
		var truncate_load = call_common_function('get_value_with_default' , common_fun_param, CONSTRAINTS_LIST);
		if (truncate_load == 'yes') {
			var truncate_cmd = `truncate table `+ database_name + `.` + schema_name + `.` + table_name + `;`
            executeStatement('', truncate_cmd, []);
		}
        return truncate_load;
    }

    //----define some custome variables
    var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
    var etl_name = constraints_list_json["etl_name"].toUpperCase();
    var etl_task_name = constraints_list_json["etl_task_name"].toUpperCase();
    var executed_sp = constraints_list_json["executed_sp"].toUpperCase();
    var uploaded_date = constraints_list_json["uploaded_date"];
    var load_date = constraints_list_json["load_date"];
    var action_on_error = constraints_list_json["action_on_error"];
    var offload = constraints_list_json["offload"]

    //-----make started entry in the master audit log
    extra_info = JSON.stringify({"status":"Started", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

   snowflake.execute(
       {
           sqlText: `BEGIN TRANSACTION`
       }
   );

    var database_name = constraints_list_json["database_name"].toUpperCase();
    var schema_name = constraints_list_json["schema_name"].toUpperCase();
    var table_name = constraints_list_json["table_name"].toUpperCase();
    var sf_ext_stage = constraints_list_json["sf_ext_stage"].toUpperCase();
    var file_prefix_path = constraints_list_json["file_prefix_path"];
	var load_type = constraints_list_json["load_type"].toLowerCase();

    var force_fun_param = `{"json_key":"force","default_value":"FALSE"}`;
	var force = call_common_function('get_value_with_default' , force_fun_param, CONSTRAINTS_LIST);

	var common_fun_param = `{"json_key":"purge_files","default_value":"no"}`;
	var purge_files = call_common_function('get_value_with_default' , common_fun_param, CONSTRAINTS_LIST);
    var common_fun_param = `{"sf_ext_stage":"` + sf_ext_stage + `"}`;
    var file_format = call_common_function('fetch_file_format_from_extstage' , common_fun_param, '');

    var common_fun_param = `{"json_key":"validate_errors","default_value":"no"}`;
	var validate_errors = call_common_function('get_value_with_default' , common_fun_param, CONSTRAINTS_LIST);


    if (load_type == 'all_with_add_on') {
        //----fetch all columns of the table EXECUTION_ID
        var common_fun_param = `{"execution_id":"` + EXECUTION_ID +
                            `","file_format":"` + file_format +
                            `"}`;
	    var query_columns = call_common_function('fetch_table_columns' , common_fun_param, CONSTRAINTS_LIST);

        //----prepare copy command
        var copy_cmd = `COPY INTO ` + schema_name + `.` + table_name + ` FROM
                        (
                        SELECT `+ query_columns + ` FROM @` + sf_ext_stage + `/` + file_prefix_path + `
                        )
                        FORCE = `+force+`
                        ON_ERROR = '`+action_on_error+`'`
    }

    else if (load_type == 'custom_cols') {
        //----get the query columns from mapping table
        var query_cmd = `SELECT ARRAY_TO_STRING(QUERY_COLUMNS,',') FROM COM.LOADING_MAP_TBL
                        WHERE DATABASE_NAME='`+ database_name + `' AND TABLE_NAME = '` + schema_name + `.` + table_name + `';`;
        var exec_res_out = executeStatement('', query_cmd, []);
        var query_columns_res = exec_res_out.res_out;
        query_columns_res.next();
        var query_columns = query_columns_res.getColumnValue(1);

        //----prepare copy command
        var copy_cmd = `COPY INTO ` + schema_name + `.` + table_name + ` FROM
                        (
                        SELECT `+ query_columns + ` FROM @` + sf_ext_stage + `/` + file_prefix_path + `)
                        FORCE = `+force+`
                        ON_ERROR = '`+action_on_error+`'`
    }

    else if (load_type == 'as_is') {
        //----prepare copy command
        var copy_cmd = `COPY INTO ` + schema_name + `.` + table_name + ` FROM @` + sf_ext_stage + `/` + file_prefix_path + `
                        FORCE = `+force+`
                        ON_ERROR = '`+action_on_error+`'`
    }

    else {
        throw new Error("load_type is not defined, please verify the same :" + load_type);
    }


	if (purge_files == 'yes') {
		purge_files_cmd = ` purge = true `;
	}
	else{
		purge_files_cmd = ` purge = false `;
	}

    copy_cmd = copy_cmd + purge_files_cmd + ` ;`

    //---call truncate_table_before_load function before copy command
	var truncate_load_status = truncate_table_before_load();

    //---execute the copy command
    var exec_res_out = executeStatement('', copy_cmd, []);
    var query_id = exec_res_out.query_id;

    //--get the copy command query execution history
	var common_fun_param = `{"query_id":"` + query_id + `","query_type":"COPY"}`;
    var dtls_after_exec = call_common_function('get_query_history_result' , common_fun_param, '');


    dtls_after_exec = JSON.parse(dtls_after_exec);



    var success_files_count = 0;
    var error_files_count = 0;

    for (var col_num = 0; col_num < dtls_after_exec.length; col_num = col_num + 1) {
        var dtls_after_exec_json = dtls_after_exec[col_num];
        var load_status = dtls_after_exec_json["LOAD_STATUS"];
        dtls_after_exec_json["EXECUTED_SP"] = executed_sp;
        dtls_after_exec_json["SOURCE"] = 'S3';
        dtls_after_exec_json["DESTINATION"] = schema_name + `.` + table_name;
        dtls_after_exec_json["UPLOADED_DATE"] = uploaded_date;
        dtls_after_exec_json["LOAD_DATE"] = load_date;
        if (load_status == 'LOADED' || load_status == 'PARTIALLY_LOADED') {
            success_files_count = success_files_count + 1;
        }
        else {
            if ( load_status == 'LOAD_FAILED' || (dtls_after_exec_json.hasOwnProperty('MESSAGE') && !dtls_after_exec_json["MESSAGE"].includes("0 files processed"))){
                error_files_count = error_files_count + 1;
            }
        }
    }


    if(error_files_count > 0 && action_on_error != 'continue') {
        snowflake.execute({ sqlText: `ROLLBACK` });
        var return_idntfr = 'Error';
    }
    else{
        snowflake.execute({ sqlText: `COMMIT` });
        var return_idntfr = 'Success';
    }

    if ( validate_errors == 'yes' && (load_type == 'as_is'|| (load_type == 'all_with_add_on' && file_format == 'CSV') )){
        var validate_cmd = `INSERT INTO  COM.SAVE_COPY_ERROR  SELECT *,'`+EXECUTION_ID+`'
                        FROM TABLE(VALIDATE(`+schema_name+`.`+table_name+`, job_id=>'`+query_id+`'));`
        executeStatement('', validate_cmd, []);

                          }

    var common_fun_param = `{"execution_id":"` + EXECUTION_ID +
                            `","etl_name":"` + etl_name +
                            `","etl_task_name":"` + etl_task_name +
                            `","executed_sp":"` + executed_sp +
                            `","start_time":"` + START_TIME +
                            `","status":"Success"
                            }`;

    if(offload == 'yes') {
		var offload_constraints_list_json = constraints_list_json;
		offload_constraints_list_json["table_name"] = constraints_list_json["error_table_name"];
		offload_constraints_list_json["schema_name"] = constraints_list_json["error_schema_name"];
		offload_constraints_list = JSON.stringify(offload_constraints_list_json);
        var off_load_RowCount = [];
        var stmt = snowflake.createStatement({
        sqlText: 'CALL COM.GENERIC_OFFLOAD_TABLE_SP(:1,:2,:3) ;',
        binds: [EXECUTION_ID, START_TIME,offload_constraints_list]
            });

        var result = stmt.execute();
        result.res_out;
        result.next()
        off_load_RowCount.push(result.getColumnValue(1))


        var return_msg = return_idntfr + ' -> Number of files loaded: ' + success_files_count + ' and Number of files not loaded(error): ' + error_files_count +' and ' + off_load_RowCount;

    }
	else{
	var return_msg = return_idntfr + ' -> Number of files loaded: ' + success_files_count + ' and Number of files not loaded(error): ' + error_files_count;

	}


    dtls_after_exec = JSON.stringify(dtls_after_exec);
	call_common_function('insert_into_process_audit_log' , common_fun_param, dtls_after_exec);

    //-----make Completion entry in the master audit log
    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

    return return_msg;
}
catch (err) {

    var last_query_cmd =  `select last_query_id()`;
    var exec_res_out = executeStatement('', last_query_cmd, []);
    var last_query_res = exec_res_out.res_out;
    last_query_res.next();
    var error_query_id = last_query_res.getColumnValue(1);

    snowflake.execute({ sqlText: `ROLLBACK` });

    var message = err.message.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
	var stack_trace_txt = "";
    if(err.stackTraceTxt){
        stack_trace_txt = err.stackTraceTxt.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
    }
    var dtls_after_exec = JSON.stringify(
        {
            "FAIL_CODE": err.code,
            "STATE": err.state,
            "MESSAGE": message,
            "STACK_TRACE": stack_trace_txt,
            "QUERY_ID":error_query_id,
            "EXECUTED_SP" : executed_sp,
            "EXECUTION_STATUS": "FAILURE"
        }
    );

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
}
$$;
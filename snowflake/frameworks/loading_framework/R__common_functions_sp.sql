CREATE OR REPLACE PROCEDURE COM.COMMON_FUNCTIONS_SP("FUNCTION_NAME" VARCHAR(16777216), "CONVERTED_ARGS_JSON" VARCHAR(16777216), "DEFAULT_ARGS_JSON" VARCHAR(16777216))
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

    //----function to insert record in MASTER_AUDIT_LOG table
    function insert_into_master_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status, start_time, extra_info) {
        var end_time_cmd = `SELECT CURRENT_TIMESTAMP() FROM DUAL;`;
        var exec_res_out = executeStatement('', end_time_cmd, []);
        var end_time_res = exec_res_out.res_out;
        end_time_res.next();
        var end_time = end_time_res.getColumnValue(1);
        var mstr_call_cmd = 'CALL COM.GENERIC_INSERT_MASTER_AUDIT_LOG_SP(:1,:2,:3,:4,:5,:6,:7,:8);';
        var binding_vars = [execution_id,etl_name,etl_task_name,executed_sp,status, start_time, end_time,extra_info];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;
    }
    //----function to insert record in PROCESS_AUDIT_LOG table
    function insert_into_process_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status, start_time, dtls_after_exec) {
        var end_time_cmd = `SELECT CURRENT_TIMESTAMP() FROM DUAL;`;
        var exec_res_out = executeStatement('', end_time_cmd, []);
        var end_time_res = exec_res_out.res_out;
        end_time_res.next();
        var end_time = end_time_res.getColumnValue(1);
        var mstr_call_cmd = 'CALL COM.GENERIC_INSERT_PROCESS_AUDIT_LOG_SP(:1,:2,:3,:4,:5,:6,:7,:8);';
        var binding_vars = [execution_id,etl_name,etl_task_name,executed_sp,status, start_time, end_time, dtls_after_exec];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;
    }
	
	//----function to insert record in PROCESS_AUDIT_LOG table by using get_query_history_result
    function update_process_audit_log_test(args_list_json, default_args_list_json) {
        var query_id = args_list_json["query_id"];
		var query_type = args_list_json["query_type"].toUpperCase();
		var status = args_list_json["status"];
		var start_time = args_list_json["start_time"];
        var destination = args_list_json["destination"].toUpperCase();
        var execution_id = default_args_list_json["execution_id"];
        var etl_name = default_args_list_json["etl_name"].toUpperCase();
        var etl_task_name = default_args_list_json["etl_task_name"].toUpperCase();
        var executed_sp = default_args_list_json["executed_sp"].toUpperCase();
		//return query_id + " : " + query_type;
		var dtls_after_exec = get_query_history_result(query_id,query_type) ;
        //return dtls_after_exec;
		dtls_after_exec = JSON.parse(dtls_after_exec) 
        dtls_after_exec = dtls_after_exec[0]		
        dtls_after_exec["EXECUTED_SP"] = executed_sp;   
        dtls_after_exec["DESTINATION"] = destination;
        dtls_after_exec["UPLOADED_DATE"] = default_args_list_json["uploaded_date"];   
        dtls_after_exec["LOAD_DATE"] = default_args_list_json["load_date"];
		dtls_after_exec = JSON.stringify(dtls_after_exec);
		return dtls_after_exec;
		var out = insert_into_process_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status, start_time, dtls_after_exec);
		return out;	
    }


    //----function to insert record in PROCESS_AUDIT_LOG table by using get_query_history_result
    function update_process_audit_log(args_list_json, default_args_list_json) {
        var query_id = args_list_json["query_id"];
		var query_type = args_list_json["query_type"].toUpperCase();
		var status = args_list_json["status"];
		var start_time = args_list_json["start_time"];
        var destination = args_list_json["destination"].toUpperCase();
        var execution_id = default_args_list_json["execution_id"];
        var etl_name = default_args_list_json["etl_name"].toUpperCase();
        var etl_task_name = default_args_list_json["etl_task_name"].toUpperCase();
        var executed_sp = default_args_list_json["executed_sp"].toUpperCase();
		
		var dtls_after_exec = get_query_history_result(query_id,query_type) ;		
		dtls_after_exec = JSON.parse(dtls_after_exec) 
        dtls_after_exec = dtls_after_exec[0]		
        dtls_after_exec["EXECUTED_SP"] = executed_sp;   
        dtls_after_exec["DESTINATION"] = destination;
        dtls_after_exec["UPLOADED_DATE"] = default_args_list_json["uploaded_date"];   
        dtls_after_exec["LOAD_DATE"] = default_args_list_json["load_date"];
		dtls_after_exec = JSON.stringify(dtls_after_exec);

		var out = insert_into_process_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status, start_time, dtls_after_exec);
		return out;	
    }

    //------function to fetch the value with default value
    function get_value_with_default(json_text, json_key, default_value) {
        var select_cmd = `SELECT COM.GENERIC_GET_VALUE_WITH_DEFAULT_SP(parse_json('` + json_text + `'),:1,:2)`;
        var binding_vars = [json_key, default_value];
        var exec_res_out = executeStatement('binding_vars', select_cmd, binding_vars);
        var value_res = exec_res_out.res_out;
        value_res.next();
        return value_res.getColumnValue(1);
    }
	
    //----function to fetch the query history details
    function get_query_history_result(query_id,query_type) {
        var query_hist_cmd = 'CALL COM.GENERIC_GET_QUERY_HISTORY_SP(:1,:2)';
        var binding_vars = [query_id, query_type];
        var exec_res_out = executeStatement('binding_vars', query_hist_cmd, binding_vars);
        var query_hist_result = exec_res_out.res_out;
        query_hist_result.next();
        var dtls_after_exec = query_hist_result.getColumnValue(1);
        return JSON.stringify(dtls_after_exec);
        }	
		
	//----function to fetch FETCH_TABLE_COLUMNS
	function fetch_table_columns(execution_id,file_format,constraint_list){
	    var query_cmd = 'CALL COM.GENERIC_FETCH_TABLE_COLUMNS_SP(:1,:2,:3)';
	    var binding_vars = [execution_id, file_format, constraint_list];
        var exec_res_out = executeStatement('binding_vars', query_cmd, binding_vars);
        var query_columns_res = exec_res_out.res_out;
        query_columns_res.next();
        var query_columns = query_columns_res.getColumnValue(1);
		return query_columns;
	}	

	//----function to FETCH_FILE_FORMAT_FROM_EXTSTAGE
	function fetch_file_format_from_extstage(sf_ext_stage){
	    var query_cmd = 'CALL COM.GENERIC_FETCH_FILE_FORMAT_FROM_EXTSTAGE_SP(:1)';
	    var binding_vars = [sf_ext_stage];
        var exec_res_out = executeStatement('binding_vars', query_cmd, binding_vars);
        var query_columns_res = exec_res_out.res_out;
        query_columns_res.next();
        var query_columns = query_columns_res.getColumnValue(1);
		return query_columns;
	}

    if(FUNCTION_NAME.toLowerCase() == 'insert_into_master_audit_log'){
		var extra_info = JSON.parse(DEFAULT_ARGS_JSON);
		var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
        var execution_id = args_list_json["execution_id"];
		var start_time = extra_info["start_time"];
		var status = extra_info["status"];
        var etl_name = args_list_json["etl_name"];
        var etl_task_name = args_list_json["etl_task_name"];
        var executed_sp = args_list_json["executed_sp"];
		extra_info["load_date"] = args_list_json["load_date"];
		extra_info = JSON.stringify(extra_info)
		var out = insert_into_master_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status,start_time, extra_info);
		return out;
    }
    else if(FUNCTION_NAME.toLowerCase() == 'insert_into_process_audit_log'){
		var dtls_after_exec = DEFAULT_ARGS_JSON;
		var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
        var execution_id = args_list_json["execution_id"];
		var start_time = args_list_json["start_time"];
		var status = args_list_json["status"];
        var etl_name = args_list_json["etl_name"];
        var etl_task_name = args_list_json["etl_task_name"];
        var executed_sp = args_list_json["executed_sp"];
		var out = insert_into_process_audit_log(execution_id,etl_name,etl_task_name,executed_sp,status, start_time, dtls_after_exec);
		return out;
	}
    else if(FUNCTION_NAME.toLowerCase() == 'update_process_audit_log'){
		var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
		var default_args_list_json = JSON.parse(DEFAULT_ARGS_JSON);
		var out = update_process_audit_log(args_list_json,default_args_list_json);
		return out;		
	}
    else if(FUNCTION_NAME.toLowerCase() == 'get_value_with_default'){
        var json_text = DEFAULT_ARGS_JSON;
		var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
		var json_key = args_list_json["json_key"];
		var default_value = args_list_json["default_value"];
		var out = get_value_with_default(json_text, json_key, default_value);
		return out;
    }
	else if(FUNCTION_NAME.toLowerCase() == 'get_query_history_result'){
		var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
        var query_id = args_list_json["query_id"];
		var query_type = args_list_json["query_type"];
		var out = get_query_history_result(query_id,query_type) ;
		return out;
    }  
    else if(FUNCTION_NAME.toLowerCase() == 'fetch_table_columns'){
	    var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
        var execution_id = args_list_json["execution_id"];
        var file_format = args_list_json["file_format"];
		var constraint_list = DEFAULT_ARGS_JSON;
		var out = fetch_table_columns(execution_id,file_format,constraint_list) ;
		return out;
	}
    else if(FUNCTION_NAME.toLowerCase() == 'fetch_file_format_from_extstage'){
    var args_list_json = JSON.parse(CONVERTED_ARGS_JSON);
    var sf_ext_stage = args_list_json["sf_ext_stage"];
    var out = fetch_file_format_from_extstage(sf_ext_stage) ;
    return out;
    }
    else{
        return "";
    }
$$;
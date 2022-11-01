CREATE OR REPLACE PROCEDURE COM.GENERIC_COMPARE_TABLES_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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

    //-------function to compare the two rows
    function compare_result(agg_value){
        if((agg_value.length == 0) || ((agg_value.split(",").length == 2) && 
            (agg_value.split(",")[0] == agg_value.split(",")[1]))){
            return true;
        }
        return false;
    }
    
    //----define some custom variables
    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var etl_name = constraint_list_json["etl_name"].toUpperCase();
    var etl_task_name = constraint_list_json["etl_task_name"].toUpperCase();
    var upload_date = constraint_list_json["upload_date"];
    var load_date = constraint_list_json["load_date"];
    var execution_id = constraint_list_json["execution_id"];
    var executed_sp = constraint_list_json["executed_sp"].toUpperCase();
    var source_database_name = constraint_list_json["source_database_name"].toUpperCase();
    var source_schema_name = constraint_list_json["source_schema_name"].toUpperCase();
    var source_table_name = constraint_list_json["source_table_name"].toUpperCase();
    var target_database_name = constraint_list_json["target_database_name"].toUpperCase();
    var target_schema_name = constraint_list_json["target_schema_name"].toUpperCase();
    var target_table_name = constraint_list_json["target_table_name"].toUpperCase();
    var log_database_name = constraint_list_json["log_database_name"].toUpperCase();
    var log_schema_name = constraint_list_json["log_schema_name"].toUpperCase();
    var log_table_name = constraint_list_json["log_table_name"].toUpperCase();
    
    var common_fun_param = `{"json_key":"column_names_caps","default_value":"yes"}`;
    
    //-----make started entry in the master audit log
    extra_info = JSON.stringify({"status":"Started", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
    snowflake.execute(
        {
            sqlText: `BEGIN TRANSACTION`
        }
    );
   
    var select_cmd = `SELECT COLUMN_NAME,DATA_TYPE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = :1
                    AND TABLE_SCHEMA= :2
                    AND TABLE_NAME = :3
                    ;`;

    var select_out = executeStatement('binding_vars', select_cmd, [target_database_name, target_schema_name, target_table_name])

    var resOut = select_out.res_out;
    var select_out_array = [];
    var number_column_list = [];
    var timestamp_column_list = [];

    while (resOut.next()) {
        var col_name = resOut.COLUMN_NAME;
        var data_type = resOut.DATA_TYPE;
        if(data_type == 'NUMBER') {
            number_column_list.push(col_name);
        }
        else if(data_type == 'TIMESTAMP_NTZ') {
            timestamp_column_list.push(col_name);
        }
    }

    // CODE FOR EXECUTING VALIDATION ON COLUMNS

    src_table_name = source_database_name + "." + source_schema_name + "." + source_table_name;
    tgt_table_name = target_database_name + "." + target_schema_name + "." + target_table_name;
    var overall_matched = true;
    for(var column_index = 0; column_index < number_column_list.length; column_index++ ){
        var col = number_column_list[column_index];
        var select_cmd = `select 
                            listagg(MIN_VALUE,',') as MIN_VALUE, 
                            listagg(MAX_VALUE,',') as MAX_VALUE,
                            listagg(AVG_VALUE,',') as AVG_VALUE, 
                            listagg(SUM_VALUE,',') as SUM_VALUE,
                            listagg(RECORD_COUNT,',') as RECORD_COUNT,
                            listagg(DISTINCT_CNT,',') as DISTINCT_CNT from
                            (
                                select max(`+ col +`) as MAX_VALUE, min(`+ col +`) as MIN_VALUE,
                                avg(`+ col +`) as AVG_VALUE, count(*) as RECORD_COUNT,sum(`+ col +`) as SUM_VALUE,
                                count(distinct(`+ col +`)) as DISTINCT_CNT 
                                from `+ src_table_name+`
                                union all
                                select max(`+ col +`) as MAX_VALUE, min(`+ col +`) as MIN_VALUE,
                                avg(`+ col +`) as AVG_VALUE, count(*) as RECORD_COUNT,sum(`+ col +`) as SUM_VALUE,
                                count(distinct(`+ col +`)) as DISTINCT_CNT 
                                from `+ tgt_table_name+`
                            );`;

        var select_out = executeStatement('binding_vars', select_cmd, [number_column_list[column_index]]);

        var resOut = select_out.res_out;
        resOut.next();
        // Compare statistics
        var matched = false;
        if(compare_result(resOut.MIN_VALUE) && compare_result(resOut.MAX_VALUE) && 
                compare_result(resOut.AVG_VALUE) && compare_result(resOut.SUM_VALUE) &&
                compare_result(resOut.RECORD_COUNT) && compare_result(resOut.DISTINCT_CNT) ){
                    matched = true;
        } else {
            var log_tbl = log_database_name + "." + log_schema_name + "." + log_table_name;
            var insert_stmt = `insert into ` + log_tbl + ` values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)`;
            var binding_var = [number_column_list[column_index],src_table_name,tgt_table_name,
            resOut.MIN_VALUE,resOut.MAX_VALUE,resOut.AVG_VALUE,resOut.SUM_VALUE,
            resOut.RECORD_COUNT,resOut.DISTINCT_CNT,EXECUTION_ID];
            var insert_out = executeStatement('binding_vars', insert_stmt, binding_var);
            var insertResOut = insert_out.res_out;
            insertResOut.next();
            overall_matched = false;
        } 
    }

    // CODE FOR TIMESTAMP
    for(var column_index = 0; column_index < timestamp_column_list.length; column_index++ ){
        var col = timestamp_column_list[column_index];
        var select_cmd = `select 
                            listagg(MIN_VALUE,',') as MIN_VALUE, 
                            listagg(MAX_VALUE,',') as MAX_VALUE,
                            listagg(RECORD_COUNT,',') as RECORD_COUNT,
                            listagg(DISTINCT_CNT,',') as DISTINCT_CNT from
                            (
                                select max(`+ col +`) as MAX_VALUE, min(`+ col +`) as MIN_VALUE,
                                count(*) as RECORD_COUNT, count(distinct(`+ col +`)) as DISTINCT_CNT 
                                from `+ src_table_name+`
                                union all
                                select max(`+ col +`) as MAX_VALUE, min(`+ col +`) as MIN_VALUE,
                                count(*) as RECORD_COUNT, count(distinct(`+ col +`)) as DISTINCT_CNT 
                                from `+ tgt_table_name+`
                            );`;

        var select_out = executeStatement('binding_vars', select_cmd, [timestamp_column_list[column_index]]);

        var resOut = select_out.res_out;
        resOut.next();
        // Compare statistics
        var matched = false;
        if(compare_result(resOut.MIN_VALUE) && compare_result(resOut.MAX_VALUE) && 
                compare_result(resOut.RECORD_COUNT) && compare_result(resOut.DISTINCT_CNT) ){
                    matched = true;
        } else {
            var log_tbl = log_database_name + "." + log_schema_name + "." + log_table_name;
            var insert_stmt = `insert into ` + log_tbl + ` values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10)`;
            var binding_var = [timestamp_column_list[column_index],src_table_name,tgt_table_name,
            resOut.MIN_VALUE,resOut.MAX_VALUE,"","",resOut.RECORD_COUNT,resOut.DISTINCT_CNT,EXECUTION_ID];
            var insert_out = executeStatement('binding_vars', insert_stmt, binding_var);
            var insertResOut = insert_out.res_out;
            insertResOut.next();
            overall_matched = false;
        }
    }
    snowflake.execute({ sqlText: `COMMIT` });

    //-----make Completion entry in the master audit log
    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info); 

    return 'Success -> File Difference Generated. Matched Result : ' + overall_matched;
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
    
    //-----make Failed entry in the master audit log
    extra_info = JSON.stringify({"status":"Failed", "start_time": START_TIME});
	call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);

    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
}
$$;
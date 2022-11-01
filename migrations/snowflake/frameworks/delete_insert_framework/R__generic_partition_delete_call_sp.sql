CREATE OR REPLACE PROCEDURE COM.GENERIC_PARTITION_DELETE_CALL_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try {

    //---- Common function to execute the statement
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
        var binding_vars = [function_name, converted_args_json, default_args_json];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;
	}

    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var etl_name = constraint_list_json["etl_name"].toUpperCase();
    var etl_task_name = constraint_list_json["etl_task_name"].toUpperCase();;
    var upload_date = constraint_list_json["upload_date"];
    var load_date = constraint_list_json["load_date"];
    var scd_maping_schema_name = constraint_list_json["scd_maping_schema_name"].toUpperCase();
    var scd_mapping_table_name = constraint_list_json["scd_mapping_table_name"].toUpperCase();
    var source_schema_name = constraint_list_json["source_schema_name"].toUpperCase();
    var source_table_name = constraint_list_json["source_table_name"].toUpperCase();
    
    var select_cmd = `SELECT *
                    FROM `+ scd_maping_schema_name +`.` + scd_mapping_table_name+`
                    WHERE SRC_SCHEMA = '`+ source_schema_name +`'
                    AND SRC_TABLE= '`+ source_table_name +`'
                    ;`;

    var select_out = executeStatement('binding_vars', select_cmd, [scd_maping_schema_name,scd_mapping_table_name, source_schema_name, source_table_name])

    var resOut = select_out.res_out;

    var tgt_schema = '';
    var tgt_table = '';
    var primary_key_cols = '';
    var scd_cols = '';
    var job_name = '';
    var is_delete_data = '';
    var is_conditional_delete = '';

    while (resOut.next()) {
        tgt_schema = resOut.TGT_SCHEMA;
        tgt_table = resOut.TGT_TABLE;
        primary_key_cols = resOut.PRIMARY_KEY_COLS;
        scd_cols = resOut.SCD_COLS;
        job_name = resOut.JOB_NAME;
        is_delete_data = resOut.IS_DELETE_DATA;
        is_conditional_delete = resOut.IS_CONDITIONAL_DELETE;
    }
    // CODE FOR CALLING SCD SP WITH PARAMS

    if (tgt_schema != ''){

        var constraint_list_upload_target_json = constraint_list_json;
        constraint_list_upload_target_json["etl_name"] = etl_name;
		constraint_list_upload_target_json["etl_task_name"] = etl_task_name;
        constraint_list_upload_target_json["executed_sp"] = "COM.DATA_LOAD_WITH_DELETE_SP";
        constraint_list_upload_target_json["uploaded_date"] = upload_date;
        constraint_list_upload_target_json["execution_id"] = EXECUTION_ID;
        constraint_list_upload_target_json["src_schema"] = source_schema_name;
        constraint_list_upload_target_json["src_table_name"] = source_table_name;
        constraint_list_upload_target_json["tgt_schema"] = tgt_schema;
        constraint_list_upload_target_json["tgt_table_name"] = tgt_table;
        constraint_list_upload_target_json["primary_key_cols"] = primary_key_cols;
        constraint_list_upload_target_json["scd_cols"] = scd_cols;
        constraint_list_upload_target_json["etl_table_job_name"] = job_name;
        constraint_list_upload_target_json["is_delete_data"] = is_delete_data;
        constraint_list_upload_target_json["is_conditional_delete"] = is_conditional_delete;

        constraint_list_upload_target = JSON.stringify(constraint_list_upload_target_json);

        var sql_stmt = `CALL COM.DATA_LOAD_WITH_DELETE_SP(:1,:2);`;
        var scd_res_out = executeStatement('binding_vars', sql_stmt, [START_TIME,constraint_list_upload_target]);
        var scd_resOut = scd_res_out.res_out;
        scd_resOut.next();
        var scd_result = scd_resOut.getColumnValueAsString(1);
    }else{
        var scd_result = "Configuration not found in SCD mapping table.";
    }
	return 'SUCCESS -> SCD Partition Delete Called Successfully. ' + scd_result;
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
            "EXECUTED_SP" : constraints_list_json["executed_sp"],
            "EXECUTION_STATUS": "FAILURE"
        }
    );
    
    var common_fun_param = `{"execution_id":"` + constraints_list_json["execution_id"] + 
                            `","etl_name":"` + constraints_list_json["etl_name"] + 
                            `","etl_task_name":"` + constraints_list_json["etl_task_name"] + 
                            `","executed_sp":"` + constraints_list_json["executed_sp"] + 
                            `","start_time":"` + START_TIME +
                            `","status":"Failure"
                            }`;

    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
}
$$;
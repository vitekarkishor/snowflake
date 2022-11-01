CREATE OR REPLACE PROCEDURE COM.DATA_LOAD_WITH_DELETE_SP("START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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
	 
    var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
    var execution_id = constraints_list_json["execution_id"];

 	//---- Makes START entry in Master Audit Log
    extra_info = JSON.stringify({"status":"Started","start_time": START_TIME, "execution_id": execution_id });
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
    
	//---- Fetch data from Constraint_List
    var src_schema = constraints_list_json["src_schema"].toUpperCase();
	var tgt_schema = constraints_list_json["tgt_schema"].toUpperCase();
    var src_table_name = constraints_list_json["src_table_name"].toUpperCase();
	var tgt_table_name = constraints_list_json["tgt_table_name"].toUpperCase();
	var etl_job_name = constraints_list_json["etl_table_job_name"].toUpperCase();
	var is_delete_data = constraints_list_json["is_delete_data"].toUpperCase();
	    
    var select_raw_cmd = `SELECT COUNT(1) FROM ` + src_schema + `.` + src_table_name + `;`;
    var select_raw_out = executeStatement('', select_raw_cmd, [])

    var resRawOut = select_raw_out.res_out;
    resRawOut.next();
    var rawCount = resRawOut.getColumnValue(1);
    if (rawCount == 0){
        //-----make Completion entry in the master audit log
        extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
        call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
        return 'Success -> No processing done as _stg having zero records.';
    }

    var delete_result = "NOT NEEDED";
    //---- Delete data if needed
    if(is_delete_data == 'YES') {        
	    var mstr_call_cmd = 'CALL COM.DELETE_PARTITION_DATA_SP(:1,:2);';
        var binding_vars = [START_TIME, CONSTRAINTS_LIST]
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd,binding_vars);
        var delete_query_result = exec_res_out.res_out;
        delete_query_result.next();
        // Handle error here
        delete_result = delete_query_result.getColumnValueAsString(1);        
    }

    snowflake.execute(
        {
            sqlText: `BEGIN TRANSACTION`
        }
    );

    if (delete_result.includes("Error")){
            throw new Error(delete_result);
    }

    //---- Get column names of source and target table
	var col_list_query = `SELECT               
                        LISTAGG(CONCAT('SRC.', COLUMN_NAME),',') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS SRC_COLUMN_LIST, 
                        LISTAGG(COLUMN_NAME,',') WITHIN GROUP (ORDER BY ORDINAL_POSITION) AS TGT_COLUMN_LIST
                        FROM INFORMATION_SCHEMA.COLUMNS WHERE UPPER(TABLE_NAME) = '${src_table_name}' AND UPPER(TABLE_SCHEMA) = '${src_schema}'
                        AND COLUMN_NAME
                        NOT IN ('ETL_INSERT_JOB_NAME','ETL_INSERT_JOB_DATETIME');`;

    var col_list_query_object = executeStatement('', col_list_query, []);
    var col_list_query_result = col_list_query_object.res_out;
    col_list_query_result.next();
    source_column_list = col_list_query_result.getColumnValue("SRC_COLUMN_LIST");
    target_column_list = col_list_query_result.getColumnValue("TGT_COLUMN_LIST");
    var query_id = col_list_query_object.query_id;

    //Data load query
    var data_load_cmd =`INSERT INTO ` + tgt_schema + "." + tgt_table_name +` ( 
                                    ${target_column_list},
                                    ETL_INSERT_JOB_NAME,
                                    ETL_INSERT_JOB_DATETIME
                                )
                            SELECT ${source_column_list}, '${etl_job_name}', CURRENT_TIMESTAMP()
                            FROM  ` + src_schema + "." + src_table_name +` SRC;`;


    var exec_res_out = executeStatement('', data_load_cmd, []);
    var insert_query_id = exec_res_out.query_id;
    //---- Makes SUCCESS entry in Process Audit Log
    var process_log_input_params = `{"query_id":"` + insert_query_id + `","query_type":"INSERT", "status" : "Success" , "destination" : "`+ tgt_schema +`.`+ tgt_table_name +`", "start_time": "`+ START_TIME + `"}`;
    var dtls_after_exec = call_common_function('update_process_audit_log' , process_log_input_params, CONSTRAINTS_LIST);

   	//---- Update Completed entry in Master Audit Log
    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME,"execution_id": execution_id });
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST , extra_info);
   
    snowflake.execute({ sqlText: `COMMIT` });
	return 'Success -> Successfully Inserted Data. DELETE : ' + delete_result;  
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
    
	call_common_function('insert_into_process_audit_log' , common_fun_param, dtls_after_exec);

    //-----make Failed entry in the master audit log
    extra_info = JSON.stringify({"status":"Failed", "start_time": START_TIME});
	call_common_function('insert_into_master_audit_log' ,CONSTRAINTS_LIST, extra_info);

    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
}
$$;
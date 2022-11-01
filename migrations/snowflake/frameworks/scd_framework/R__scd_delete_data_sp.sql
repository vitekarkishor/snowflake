CREATE OR REPLACE PROCEDURE COM.SCD_DELETE_DATA_SP("START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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
	
	function call_common_function(function_name,converted_args_json,default_args_json){
        var mstr_call_cmd = 'CALL COM.COMMON_FUNCTIONS_SP(:1,:2,:3) ;';
        var binding_vars = [function_name, converted_args_json, default_args_json];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        var funcResOut = resOut.getColumnValueAsString(1);
        return funcResOut;
	}

    //----define some custome variables
    var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
	constraints_list_json["executed_sp"] = "COM.SCD_DELETE_DATA_SP";
    var table_name = constraints_list_json["tgt_table_name"].toUpperCase();
    var schema_name = constraints_list_json["tgt_schema"].toUpperCase();
    var stg_table_name = constraints_list_json["src_table_name"].toUpperCase();
    var stg_schema_name = constraints_list_json["src_schema"].toUpperCase();
    var is_conditional_delete = constraints_list_json["is_conditional_delete"].toUpperCase();
	var execution_id = constraints_list_json["execution_id"];
	
	//---- Makes START entry in Master Audit Log
    extra_info = JSON.stringify({"status":"Started","start_time": START_TIME, "execution_id": execution_id });
    call_common_function('insert_into_master_audit_log' , JSON.stringify(constraints_list_json), extra_info);
	
    snowflake.execute(
        {
            sqlText: `BEGIN TRANSACTION`
        }
    );

    var mapping_table = {
        "CASE_FLEXIBLE_ATTRIBUTE" : "CASE_FLEXIBLE_ATTRIBUTE_ID",
        "CASE_SERVICE_LEVEL_AGREEMENT" : "CASE_SERVICE_LEVEL_AGREEMENT_ID",
        "ADVANCE_PAYMENT" : "ADVANCE_PAYMENT_ID",
        "CASE" : "CASE_ID",
        "BROADBAND_FIXED_LINE_ORDER_ACTION_STEP" : "SUBSCRIBER_ID" ,
        "CHARGE_TAX" : "CHARGE_TAX_ID",
        "CUSTOMER_SPENDING_LIMIT_HISTORY":"CUSTOMER_SPENDING_LIMIT_HISTORY_ID",
        "FINANCIAL_ACCOUNT_INVOICE_STAGING": "INVOICE_ID",
        "BROADBAND_FIXED_LINE_ORDER" : "ORDER_ID"
    };

    var delete_cmd = ``;
    
    if(is_conditional_delete == 'YES') {
        if(mapping_table[table_name]){
            delete_cmd = `DELETE FROM ` + schema_name + `.` + table_name + ` 
                        WHERE ` + mapping_table[table_name] +` IN (
                            SELECT TRIM(` + mapping_table[table_name] +`) FROM
                            `+ stg_schema_name + `.` + stg_table_name +` as stg
                            WHERE stg.dmlind = 'D'
                        );`;
        } else {
            throw new Error("Mapping table not found for delete.");
        }        
    } else {
        delete_cmd = `DELETE FROM ` + schema_name + `.` + table_name + `;`;
    }

    var exec_res_out = executeStatement('', delete_cmd, []);   
	var query_id = exec_res_out.query_id;
	
	//---- Makes SUCCESS entry in Process Audit Log
    var process_log_input_params = `{"query_id":"` + query_id + `","query_type":"DELETE", "status" : "SUCCESS" , "destination" : "`+ schema_name +`.`+ table_name +`", "start_time": "`+ START_TIME + `"}`;
    var dtls_after_exec = call_common_function('update_process_audit_log' , process_log_input_params, JSON.stringify(constraints_list_json));

   	//---- Update Completed entry in Master Audit Log
    extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME,"execution_id": execution_id });
    call_common_function('insert_into_master_audit_log' , JSON.stringify(constraints_list_json) , extra_info);
	
    snowflake.execute({ sqlText: `COMMIT` });

    return 'Success -> The Stored Procedure(SP_DELETE_DATA) Has Successfully Executed.' ;
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
	call_common_function('insert_into_master_audit_log' , JSON.stringify(constraints_list_json), extra_info);

    return 'Error -> Technical Error Occured ' + dtls_after_exec ;
}
$$;
CREATE OR REPLACE PROCEDURE COM.GENERIC_FETCH_ERROR_RECORDS_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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
    var skip_dq = constraint_list_json["skip_dq"].toUpperCase();
    var error_schema_name = constraint_list_json["error_schema_name"].toUpperCase();
    var error_table_name = constraint_list_json["error_table_name"].toUpperCase();
    var error_offload = constraint_list_json["error_offload"].toUpperCase();
    var upload_to_target = constraint_list_json["upload_to_target"].toUpperCase();
    var common_fun_param = `{"json_key":"column_names_caps","default_value":"yes"}`;
    
    //-----make started entry in the master audit log
    extra_info = JSON.stringify({"status":"Started", "start_time": START_TIME});
    call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
    
    snowflake.execute(
        {
            sqlText: `BEGIN TRANSACTION`
        }
    );

    var select_raw_cmd = `SELECT COUNT(1) FROM ` + source_schema_name + `.` + source_table_name + `;`;
    var select_raw_out = executeStatement('', select_raw_cmd, [])

    var resRawOut = select_raw_out.res_out;
    resRawOut.next();
    var rawCount = resRawOut.getColumnValue(1);
    if (rawCount == 0){
        //-----make Completion entry in the master audit log
        extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
        call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info); 
        snowflake.execute({ sqlText: `COMMIT` });
        return 'Success -> No processing done as _raw having zero records.';
    }
   
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

    if(skip_dq == 'NO'){

        // CODE FOR GETTING CHECK RULE NAMES

        var select_cmd = `SELECT DQ_SCHEMA || '.' || DQ_RULE || '(' || COLUMN_NAME || ') AS ' || DQ_RULE || '_' || COLUMN_NAME DQ_RULE
                        FROM 
                        COM.DQ_MAPPING_TABLE 
                        WHERE 
                        DATABASE_NAME = :1
                        AND SCHEMA_NAME = :2
                        AND TABLE_NAME = :3
                        AND DQ_RULE NOT IN ('length_chk_ud')
                        ;`;

        var select_out = executeStatement('binding_vars', select_cmd, [source_database_name, source_schema_name, source_table_name])

        var resOut = select_out.res_out;
        var select_out_array_mapping = [];

        while (resOut.next()) {
            var dq_rule = resOut.DQ_RULE;
            select_out_array_mapping.push(dq_rule);
        }

        // CODE FOR GETTING DIFFERENT CHECKS LIKE LENGTH CHECK

        var select_cmd = `SELECT                                                         
                        'LENGTH('||COLUMN_NAME||') <= '||DQ_SCHEMA||'.'||'GET_TEXT_COLUMN_LENGTH_UD'||'('\''|| :4 ||''\','\''||:5||''\','\''||:6||''\','\''||COLUMN_NAME||''\') AS '|| DQ_RULE || '_' || COLUMN_NAME
                        FROM COM.DQ_MAPPING_TABLE
                        WHERE 
                        DATABASE_NAME = :1
                        AND SCHEMA_NAME = :2
                        AND TABLE_NAME = :3
                        AND DQ_RULE = 'length_chk_ud'
                        ;`;

        var select_out = executeStatement('binding_vars', select_cmd, [source_database_name, source_schema_name, source_table_name,target_database_name,target_schema_name,target_table_name])

        var resOut = select_out.res_out;

        while (resOut.next()) {
            var dq_rule = resOut.getColumnValue(1);
            select_out_array_mapping.push(dq_rule);
        }

        var length_check_names_with_alias = select_out_array_mapping.join(" , ");
        
        // CODE FOR GETTING ALIAS NAMES

        var bool_check = "false";
        var select_cmd = `SELECT DQ_RULE ||'_' || COLUMN_NAME || ' = ' || '`+bool_check+`' AS DQ_RULE
                        FROM 
                        COM.DQ_MAPPING_TABLE 
                        WHERE DATABASE_NAME = '`+source_database_name+`' 
                        AND SCHEMA_NAME = '`+source_schema_name+`' 
                        AND TABLE_NAME = '`+source_table_name+`';`;
        
        var select_out = executeStatement('binding_vars', select_cmd, [source_database_name, source_schema_name, source_table_name])

        var resOut = select_out.res_out;
        var select_out_array_mapping = [];

        while (resOut.next()) {
            var dq_rule = resOut.DQ_RULE;
            select_out_array_mapping.push(dq_rule);
        }
        
        var filter_check_alias = select_out_array_mapping.join(" or ");
    
        // CODE FOR CHECKING DQ_RULE STORAGE ARRAY EMPTY OR NOT
        
        var select_cmd = `SELECT DQ_RULE
                        FROM 
                        COM.DQ_MAPPING_TABLE 
                        WHERE DATABASE_NAME = '`+source_database_name+`' 
                        AND SCHEMA_NAME = '`+source_schema_name+`' 
                        AND TABLE_NAME = '`+source_table_name+`';`;
        
        var select_out = executeStatement('binding_vars', select_cmd, [source_database_name, source_schema_name, source_table_name])
        
        var resOut = select_out.res_out;
        var select_out_array_mapping = [];

        while (resOut.next()) {
            var dq_rule = resOut.DQ_RULE;
            if(dq_rule.length > 0){
                select_out_array_mapping.push(dq_rule);
            }
        }
        
        // CODE FOR INSERTING ERROR RECORDS INTO ERROR TABLE
        var RowCount = 0;
        if (select_out_array_mapping.length != 0){

            var select_col = length_check_names_with_alias +','+ table_columns_name;                                                           
            var final_query = `INSERT INTO  `+error_schema_name+`.`+error_table_name+`  SELECT '`+source_database_name+`' AS DATABASE_NAME, '`+source_schema_name+`' AS SCHEMA_NAME, '`+source_table_name+`' AS TABLE_NAME, OBJECT_CONSTRUCT(*) ERROR_INFO,'`+execution_id+`'   FROM (SELECT `+select_col+` FROM `+source_database_name+`.`+source_schema_name+`.`+source_table_name+`  WHERE `+filter_check_alias+`) ;`;                    
            
            var query_output = executeStatement('', final_query, []);
            var resOut = query_output.res_out;
            resOut.next();
            RowCount = resOut.getColumnValueAsString(1);

            //--- process audit log entry---//
            var query_id = query_output.query_id;
            var process_log_input_params = `{"query_id":"` + query_id + `","query_type":"INSERT", "status" : "Success" , "destination" : "`+error_schema_name+`.`+error_table_name+`", "start_time": "`+ START_TIME + `"}`;
            var dtls_after_exec = call_common_function('update_process_audit_log' , process_log_input_params, CONSTRAINTS_LIST);  

        }


        // CODE FOR CALLING OFFLOAD STORED PROCEDURE

        if (error_offload == 'YES'){

            var error_offload_config = constraint_list_json["error_offload_config"];
            var stage_name = error_offload_config['stage_name'].toUpperCase();
            var folder_name = error_offload_config['folder_name'];
            var query_columns = error_offload_config['query_columns'].toUpperCase();
            var condition = error_offload_config['condition'].toUpperCase();
            var limit_value = error_offload_config['limit_value'].toUpperCase();

            var sql_stmt = `CALL COM.GENERIC_OFFLOAD_TABLE_SP( '`+execution_id+`'::VARCHAR,
                            CURRENT_TIMESTAMP()::TIMESTAMP_NTZ::VARCHAR,
                            '{"etl_name": "`+etl_name+`",
                            "etl_task_name": "`+etl_task_name+`",
                            "executed_sp": "COM.GENERIC_OFFLOAD_TABLE_SP",
                            "uploaded_date": "`+upload_date+`",
                            "load_date": "`+load_date+`",
                            "execution_id": "`+execution_id+`" ,
                            "stage_name": "`+stage_name+`",
                            "folder_name" : "`+folder_name+`",
                            "query_columns" : "`+query_columns+`",
                            "condition" : "`+condition+`",
                            "database_name" : "`+source_database_name+`",
                            "schema_name" : "`+error_schema_name+`",
                            "format_name" : "CSV_LOAD_FF",
                            "table_name": "`+error_table_name+`",
                            "limit_value" : "`+limit_value+`",
                            "date_format" : ""
                            }'::VARCHAR)
                            ;`;
                            
            var offload_res_out = executeStatement('',sql_stmt,[]);
            var offload_resOut = offload_res_out.res_out;
            offload_resOut.next();
            var offload_result = offload_resOut.getColumnValueAsString(1);

        }else{
            var offload_result = "";
        }

        // CODE FOR CALLING GENERIC FETCH CLEAN RECORD STORED PROCEDURE
        
        snowflake.execute({ sqlText: `COMMIT` });

        if (upload_to_target == 'YES'){
        
            var constraint_list_upload_target_json = constraint_list_json;
            var upload_to_target_config = constraint_list_upload_target_json["upload_to_target_config"];
            var upload_target_executed_sp = upload_to_target_config['executed_sp'].toUpperCase();
            constraint_list_upload_target_json["executed_sp"] = upload_target_executed_sp;
            constraint_list_upload_target_json["execution_id"] = execution_id;
            constraint_list_upload_target = JSON.stringify(constraint_list_upload_target_json);
            var insert_cmd = `CALL COM.GENERIC_FETCH_CLEAN_RECORDS_SP(:1,:2,:3) ;`;
            var clean_res_out = executeStatement('binding_vars', insert_cmd, [CONSTRAINTS_LIST,START_TIME,constraint_list_upload_target])
            var clean_resOut = clean_res_out.res_out;
            clean_resOut.next();
            var clean_result = clean_resOut.getColumnValueAsString(1);

        }else{
            var clean_result = "";
        }

        //-----make Completion entry in the master audit log
        extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
        call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info); 

        return 'Success -> DQ Rule Applied Successfully, Error Records Found - ' + RowCount + '\n' + offload_result + '\n' + clean_result;
    }else{

        //------truncate table if truncate_load is set to yes
        if (constraint_list_json.hasOwnProperty("truncate_load") && constraint_list_json["truncate_load"].toUpperCase() == 'YES' ){
            var truncate_cmd = `truncate table `+ target_database_name + `.` + target_schema_name + `.` + target_table_name + `;`
            executeStatement('', truncate_cmd, []);
        }

        // CODE FOR INSERTING SOURCE TABLE DATA INTO TARGET TABLE

        var insert_into_target = `INSERT INTO "`+target_database_name+`"."`+target_schema_name+`"."`+target_table_name+`" (`+table_columns_name+
                                `) SELECT `+table_columns_name+` FROM "`+source_database_name+`"."`+source_schema_name+`"."`+source_table_name+`";`;

        var query_output = executeStatement('', insert_into_target, []);
        var resOut = query_output.res_out;
        resOut.next();
        RowCount = resOut.getColumnValueAsString(1);

        //--- process audit log entry---//
        var query_id = query_output.query_id;
        var process_log_input_params = `{"query_id":"` + query_id + `","query_type":"INSERT", "status" : "Success" , "destination" : "`+target_schema_name+`.`+target_table_name+`", "start_time": "`+ START_TIME + `"}`;
        var dtls_after_exec = call_common_function('update_process_audit_log' , process_log_input_params, CONSTRAINTS_LIST);

        //-----make Completion entry in the master audit log
        extra_info = JSON.stringify({"status":"Completed", "start_time": START_TIME});
        call_common_function('insert_into_master_audit_log' , CONSTRAINTS_LIST, extra_info);
        
        snowflake.execute({ sqlText: `COMMIT` });

        return 'Success -> All Records - ' + RowCount + ' Inserted Into Target Table ' + target_table_name;
    }

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
CREATE OR REPLACE PROCEDURE COM.GENERIC_FETCH_TABLE_COLUMNS_SP("EXECUTION_ID" VARCHAR(16777216), "FILE_FORMAT" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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

    function get_auditcols_value(col_name,constraint_list_json){

        var file_meta_data = constraint_list_json["file_meta_data"];
        var file_row_number = constraint_list_json["file_row_number"];
        var batch_id = constraint_list_json["batch_id"];
        var insert_date_time = constraint_list_json["insert_date_time"];
        var update_date_time = constraint_list_json["update_date_time"];

        if (col_name == 'FILENAME' && file_meta_data == 'yes') {
            var fieldVal = 'METADATA$FILENAME';
        }
        else if (col_name == 'FILE_ROW_NUMBER' && file_row_number == 'yes') {
            var fieldVal = 'METADATA$FILE_ROW_NUMBER';
        }
        else if (col_name == 'BATCH_ID' && batch_id == 'yes') {
            var fieldVal = `'` + EXECUTION_ID + `'`;
        }
        else if (col_name == 'INSERT_DATE_TIME' && insert_date_time == 'yes') {
            var fieldVal = 'CURRENT_TIMESTAMP()';
        }
        else if (col_name == 'UPDATE_DATE_TIME' && update_date_time == 'yes') {
            var fieldVal = 'CURRENT_TIMESTAMP()';
        }
        else {
            var fieldVal = 'null';
        }
        return fieldVal;
    }
    function get_partitioncols_value(col_name,constraint_list_json){
        var extract_val = 'COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME'+',\''+col_name+'\')';
        if (col_datatype == 'DATE'){
            var field_val = 'TO_DATE(' + extract_val + '::TIMESTAMP)'
        }
        else if(col_datatype == 'NUMBER'){
            var field_val = extract_val + '::' + col_datatype+'('+numeric_precision+','+numeric_scale+')'
        }
        else{
            var field_val = extract_val + '::' + col_datatype
        }
        return field_val;
    }

    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var database_name = constraint_list_json["database_name"].toUpperCase();
    var schema_name = constraint_list_json["schema_name"].toUpperCase();
    var table_name = constraint_list_json["table_name"].toUpperCase();
    var file_format = FILE_FORMAT.toUpperCase();
    var common_fun_param = `{"json_key":"column_names_caps","default_value":"yes"}`;
    var column_names_caps = call_common_function('get_value_with_default' , common_fun_param, CONSTRAINTS_LIST);

    var common_fun_param = `{"json_key":"partition_cols","default_value":"[]"}`;
    var partition_cols = call_common_function('get_value_with_default' , common_fun_param, CONSTRAINTS_LIST);

    var audit_cols = ['FILE_META_DATA','FILE_ROW_NUMBER','BATCH_ID','INSERT_DATE_TIME','UPDATE_DATE_TIME']


    var select_cmd = `SELECT ORDINAL_POSITION,COLUMN_NAME, DATA_TYPE,
                        NUMERIC_PRECISION,NUMERIC_SCALE
                        FROM INFORMATION_SCHEMA.COLUMNS
                        WHERE TABLE_CATALOG = :1
                        AND TABLE_SCHEMA= :2
                        AND TABLE_NAME = :3
                        ORDER BY ORDINAL_POSITION;`

    var select_stmt = snowflake.createStatement(
            {
                    sqlText: select_cmd
                    , binds: [database_name, schema_name, table_name]
            }
    );

    var select_out = select_stmt.execute();

    var select_out_array = [];

    while (select_out.next()) {

        var col_position = select_out.ORDINAL_POSITION;
        var col_name = select_out.COLUMN_NAME;
        var col_datatype = select_out.DATA_TYPE;
        var numeric_precision = select_out.NUMERIC_PRECISION;
        var numeric_scale = select_out.NUMERIC_SCALE;
        
        if (file_format == 'CSV'){
            if (audit_cols.includes(col_name)){
                var field_val = get_auditcols_value(col_name,constraint_list_json)
            }
            else if (partition_cols.includes(col_name.toLowerCase())){
                col_name_lower = col_name.toLowerCase();
                var field_val = get_partitioncols_value(col_name_lower,constraint_list_json)
            }
            else{
                var field_val = '$' + col_position
                }
            select_out_array.push(field_val); 
            }
        else if (file_format == 'PARQUET'){
            if (audit_cols.includes(col_name)){
                var field_val = get_auditcols_value(col_name,constraint_list_json)
            }
            else if (partition_cols.includes(col_name.toLowerCase())){
                col_name_lower = col_name.toLowerCase();
                var field_val = get_partitioncols_value(col_name_lower,constraint_list_json)
            }
            else{
                if (column_names_caps == 'yes' || col_name.match(/^.*_ENCR_INT$/)){
                    col_name = col_name.toUpperCase();
                }
                else{
                    col_name = col_name.toLowerCase();
                }

                if (col_datatype == 'DATE'){
                    var field_val = 'TO_DATE($1:' + col_name + '::TIMESTAMP)'
                }
                else if(col_datatype == 'NUMBER'){
                    var field_val = '$1:' + col_name + '::' + col_datatype+'('+numeric_precision+','+numeric_scale+')'
                }
                else{
                    var field_val = '$1:' + col_name + '::' + col_datatype
                }

            }
        select_out_array.push(field_val);            
        }
        else if (['JSON','AVRO','ORC'].includes(file_format)){
            if (audit_cols.includes(col_name)){
                var field_val = get_auditcols_value(col_name,constraint_list_json)
            }
            else{
                if (column_names_caps == 'yes' || col_name.match(/^.*_ENCR_INT$/)){
                    col_name = col_name.toUpperCase();
                }
                else{
                    col_name = col_name.toLowerCase();
                }

                if (col_datatype == 'DATE'){
                    var field_val = 'TO_DATE($1:' + col_name + '::TIMESTAMP)'
                }
                else if(col_datatype == 'NUMBER'){
                    var field_val = '$1:' + col_name + '::' + col_datatype+'('+numeric_precision+','+numeric_scale+')'
                }
                else{
                    var field_val = '$1:' + col_name + '::' + col_datatype
                }

            }
        select_out_array.push(field_val);            
        }
        else{
            select_out_array.push(col_name);
        }
}

var query_columns = select_out_array.join(",");

return query_columns;
$$;
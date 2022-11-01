CREATE OR REPLACE PROCEDURE COM.CHECK_NULL_COLUMN_SP("CONSTRAINTS_LIST" VARCHAR(16777216))
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
	
	//---FETCH json AND other required variables
	var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);

   	var table_catalog = constraints_list_json["table_catalog"].toUpperCase();
   	var table_schema = constraints_list_json["table_schema"].toUpperCase();
    var table_name = constraints_list_json["table_name"].toUpperCase();  
	
    //---- QUERY FOR GETTING COLUMN NAMES
	var query_cmd = `SELECT COLUMN_NAME from information_schema.columns where TABLE_SCHEMA = '`+table_schema+`' and  TABLE_CATALOG = '`+table_catalog+`' and TABLE_NAME = '`+table_name+`';`;
    var exec_res_out = executeStatement('', query_cmd, []);
	var query_columns_res = exec_res_out.res_out;
    query_columns_res.next()

    var select_condition_array = [];
    var where_condition_array = [];
    var aaa =[]
    while(query_columns_res.next()){
        var COLUMN_NAME = query_columns_res.getColumnValue(1);
        aaa.push(COLUMN_NAME)
        var select_condition = `iff (count(`+COLUMN_NAME+`) = 0 , 'FALSE' , 'TRUE') as `+COLUMN_NAME+`_COUNT` ;
        var where_condition = COLUMN_NAME+`_COUNT = FALSE`;
        select_condition_array.push(select_condition);
        where_condition_array.push(where_condition);
    }
    
    var select_clause = select_condition_array.join(',');
    
    var where_clause = where_condition_array.join(' OR ');

    var null_query_cmd = `SELECT OBJECT_CONSTRUCT(*) from (SELECT `+select_clause+` FROM `+table_catalog+`.`+table_schema+`.`+table_name+` );`;

    var null_exec_res_out = executeStatement('', null_query_cmd, []);
    
    var null_query_columns_res = null_exec_res_out.res_out;
    
    null_query_columns_res.next();
    
    var result_object = null_query_columns_res.getColumnValue(1);
    var result_object_dic = JSON.stringify(result_object);
    result_object_dic = JSON.parse(result_object_dic);

    var null_columns = [];
    for (var key in result_object_dic){
        if (result_object_dic[key] === 'FALSE'){
            null_columns.push(key)
        }
    }
    if (null_columns.length>0){
        result = 'NULL columns present in table : '+table_name+ '\n'+null_columns;
    }else{
        result = 'No NULL columns in table : '+table_name;
    }
    return result;
}
catch (err) {
    
    var last_query_cmd =  'select last_query_id()';
    var exec_res_out = executeStatement('', last_query_cmd, []);
    var last_query_res = exec_res_out.res_out;
	last_query_res.next();
    
    return 'Error -> Technical Error Occured '+'\n Query Id : '+last_query_res.getColumnValue(1);
}
$$;
CREATE OR REPLACE PROCEDURE COM.GENERIC_TRUNCATE_DATA_CALL_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
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

    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var table_list = constraint_list_json["table_list"];
    var count = 0;
    var not_truncated_tables = [];
    for(index in table_list) {
        try {
            var truncate_cmd = `DELETE FROM `+table_list[index] +`;`;
            var truncate_out = executeStatement('', truncate_cmd, []);
            count+=1;
        } catch (err){
            not_truncated_tables.push(table_list[index]);
            continue;
        }     
    }
    if (count == table_list.length){
        return 'Success -> Data Truncated Successfully of all tables.';
    } else {
        return 'Error -> Few tables are not truncated. Not truncated tables : ' + not_truncated_tables;
    }
}
catch (err) {
    return 'Error -> Technical Error Occured ' + err ;
}
$$;
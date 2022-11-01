CREATE OR REPLACE PROCEDURE COM.GENERIC_FETCH_DDLS_SP("OBJECT_TYPE" VARCHAR(16777216), "TABLES_LIST" ARRAY)
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

    var i; 
    var funcResOut = "";
    var out =[];

    // Remember to capitalize variables input to the stored procedure definition
    for(i = 0; i < TABLES_LIST.length; i++){
        var table_name = TABLES_LIST[i];
        var mstr_call_cmd = `select get_ddl(:1,:2,'true');`;
        var binding_vars = [OBJECT_TYPE,table_name];
        var exec_res_out = executeStatement('binding_vars', mstr_call_cmd, binding_vars);
        var resOut = exec_res_out.res_out;
        resOut.next();
        funcResOut += resOut.getColumnValueAsString(1);
        funcResOut += '\n';
    }
return funcResOut;

$$;
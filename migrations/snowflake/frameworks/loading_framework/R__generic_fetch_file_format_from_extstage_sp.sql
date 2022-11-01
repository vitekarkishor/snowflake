CREATE OR REPLACE PROCEDURE COM.GENERIC_FETCH_FILE_FORMAT_FROM_EXTSTAGE_SP("SF_EXT_STAGE" VARCHAR(16777216))
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

    var query_cmd = `DESCRIBE STAGE `+SF_EXT_STAGE;
    executeStatement('', query_cmd, []);

    var query_cmd = `SELECT "property_value" 
                       FROM TABLE(RESULT_SCAN(LAST_QUERY_ID()))
                       WHERE 
                       "parent_property" = 'STAGE_FILE_FORMAT' AND 
                       "property" = 'FORMAT_NAME'`;

    var exec_res_out = executeStatement('', query_cmd, []);
    var ff_name_res_out = exec_res_out.res_out;
    ff_name_res_out.next()
    var ff_name = ff_name_res_out.getColumnValue(1);
    var query_cmd = `SELECT FILE_FORMAT_TYPE
                        FROM INFORMATION_SCHEMA.FILE_FORMATS
                        WHERE
                        ( FILE_FORMAT_CATALOG||'.'||FILE_FORMAT_SCHEMA||'.'||FILE_FORMAT_NAME = '`+ff_name+`' 
                        OR FILE_FORMAT_SCHEMA||'.'||FILE_FORMAT_NAME = '`+ff_name+`');`;

    var exec_res_out = executeStatement('', query_cmd, []);
    var file_format_res_out = exec_res_out.res_out;
    file_format_res_out.next()
    return file_format_res_out.getColumnValue(1);
$$;
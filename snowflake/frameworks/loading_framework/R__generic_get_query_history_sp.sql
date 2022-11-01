CREATE OR REPLACE PROCEDURE COM.GENERIC_GET_QUERY_HISTORY_SP("QUERYID" VARCHAR(16777216), "QUERYTYPE" VARCHAR(16777216))
RETURNS VARIANT
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

    var json_row = [];

    var cmd = `SELECT QUERY_ID,USER_NAME,QUERY_TYPE,EXECUTION_STATUS,START_TIME, END_TIME,TOTAL_ELAPSED_TIME FROM TABLE(information_schema.QUERY_HISTORY()) WHERE QUERY_ID = '` + QUERYID + `';`
    var exec_res_out = executeStatement('', cmd, []);
    var db = exec_res_out.res_out;
    db.next();

    var columns = ["QUERY_ID","USER_NAME", "QUERY_TYPE", "EXECUTION_STATUS", "START_TIME", "END_TIME", "TOTAL_ELAPSED_TIME"];

    if (QUERYTYPE == 'COPY') {
    try {
        snowflake.execute(
        {
            sqlText: `select $2 from TABLE(RESULT_SCAN('`+ QUERYID + `'));`
        }
    );    
    }
    catch(err){
        var err_msg = snowflake.execute(
        {
            sqlText: `select $1 from TABLE(RESULT_SCAN('`+ QUERYID + `'));`
        }
    ); 
    err_msg.next()
    var errResOut = err_msg.getColumnValueAsString(1);
    throw new Error(errResOut);
    
    }
        rows_cmd = `SELECT 
                    $1 AS FILE_NAME,
                    $2 AS LOAD_STATUS,
                    $3 AS SOURCE_ROWS,
                    $4 AS ROWS_LOADED,
                    $6 AS ERRORS_SEEN,
                    REPLACE($7,'''','') AS LOAD_ERROR, 
                    $8 AS LOAD_ERROR_LINE,
                    $9 AS LOAD_ERROR_CHARACTER,
                    REPLACE($10,'"','') AS LOAD_ERROR_COLUMN_NAME,
                    $4 AS ROWS_INSERTED,
                    0 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
                    FROM TABLE(RESULT_SCAN('`+ QUERYID + `'));`
    }
    else if (QUERYTYPE == 'INSERT') {
        rows_cmd = `SELECT 
					0 AS SOURCE_ROWS,
					$1 AS ROWS_LOADED,
					'' AS FILE_NAME,
					'' AS LOAD_STATUS,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    $1 AS ROWS_INSERTED,
                    0 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
					FROM TABLE(RESULT_SCAN('` + QUERYID + `'));`
    }
    else if (QUERYTYPE == 'UPDATE') {
        rows_cmd = `SELECT 
					0 AS SOURCE_ROWS,
					0 AS ROWS_LOADED,
					'' AS FILE_NAME,
					'' AS LOAD_STATUS,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    0 AS ROWS_INSERTED,
                    $1 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
					FROM TABLE(RESULT_SCAN('` + QUERYID + `'));`
    }
    else if (QUERYTYPE == 'UNLOAD') {
        rows_cmd = `SELECT 
					0 AS SOURCE_ROWS,
					$1 AS ROWS_LOADED,
					'' AS FILE_NAME,
					'UNLOADED' AS LOAD_STATUS,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    0 AS ROWS_INSERTED,
                    0 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
					FROM TABLE(RESULT_SCAN('` + QUERYID + `'));`
    }
    else if (QUERYTYPE == 'MERGE') {
        rows_cmd = `SELECT 
					0 AS SOURCE_ROWS,
					($1+$2) AS ROWS_LOADED,
					'' AS FILE_NAME,
					'MERGED' AS LOAD_STATUS,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    $1 AS ROWS_INSERTED,
                    $2 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
					FROM TABLE(RESULT_SCAN('` + QUERYID + `'));`
    }
    else if (QUERYTYPE == 'DELETE') {
        rows_cmd = `SELECT 
					0 AS SOURCE_ROWS,
					0 AS ROWS_LOADED,
					'' AS FILE_NAME,
					'DELETED' AS LOAD_STATUS,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    0 AS ROWS_INSERTED,
                    0 AS ROWS_UPDATED,
                    $1 AS ROWS_DELETED
					FROM TABLE(RESULT_SCAN('` + QUERYID + `'));`
    }
    else {
        rows_cmd = `SELECT 
					'' AS FILE_NAME,
					'' AS LOAD_STATUS,
					0 AS SOURCE_ROWS,
					0 AS ROWS_LOADED,
					0 AS ERRORS_SEEN, 
					'' AS LOAD_ERROR, 
					'' AS LOAD_ERROR_LINE, 
					'' AS LOAD_ERROR_CHARACTER, 
					'' AS LOAD_ERROR_COLUMN_NAME,
                    0 AS ROWS_INSERTED,
                    0 AS ROWS_UPDATED,
                    0 AS ROWS_DELETED
                    ;`
    }

    var exec_res_out = executeStatement('', rows_cmd, []);
    var rows_loaded_set = exec_res_out.res_out;

    while (rows_loaded_set.next()) {
        var json_element = {};
        json_element["FILE_NAME"] = rows_loaded_set.getColumnValue('FILE_NAME');
        json_element["LOAD_STATUS"] = rows_loaded_set.getColumnValue('LOAD_STATUS');
        json_element["SOURCE_ROWS"] = rows_loaded_set.getColumnValue('SOURCE_ROWS');
        json_element["ROWS_LOADED"] = rows_loaded_set.getColumnValue('ROWS_LOADED');
        json_element["ERRORS_SEEN"] = rows_loaded_set.getColumnValue('ERRORS_SEEN');
        json_element["LOAD_ERROR"] = rows_loaded_set.getColumnValue('LOAD_ERROR');
        json_element["LOAD_ERROR_LINE"] = rows_loaded_set.getColumnValue('LOAD_ERROR_LINE');
        json_element["LOAD_ERROR_CHARACTER"] = rows_loaded_set.getColumnValue('LOAD_ERROR_CHARACTER');
        json_element["LOAD_ERROR_COLUMN_NAME"] = rows_loaded_set.getColumnValue('LOAD_ERROR_COLUMN_NAME');
        json_element["ROWS_INSERTED"] = rows_loaded_set.getColumnValue('ROWS_INSERTED');
        json_element["ROWS_UPDATED"] = rows_loaded_set.getColumnValue('ROWS_UPDATED');
        json_element["ROWS_DELETED"] = rows_loaded_set.getColumnValue('ROWS_DELETED');

        for (var col_num = 0; col_num < columns.length; col_num = col_num + 1) {
            var col_name = columns[col_num];
            json_element[col_name] = db.getColumnValue(col_num + 1);
        }
        json_row.push(json_element);
    }

    return json_row;
}
catch (err) {
    var message = err.message.replace(/\n/g, " ").replace(/'/g, "").replace(/\r/g, " ").replace(/"/g, " ");
    json_row = [{
        "FAIL_CODE": err.code,
        "STATE": err.state,
        "MESSAGE": message,
        "STACK_TRACE": err.stackTraceTxt
    }];
    return json_row;
}
$$;
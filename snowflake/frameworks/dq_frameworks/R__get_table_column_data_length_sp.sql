CREATE OR REPLACE PROCEDURE COM.GET_TABLE_COLUMN_DATA_LENGTH_SP("CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

    function executeStatement(exec_type, query_cmd, binding_vars){
        if (exec_type == 'binding_vars') {
            var query_stmt = snowflake.createStatement(
            {
                sqlText: query_cmd
                , binds: binding_vars
            }
            );
        }
        else{
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
    var db_name = constraint_list_json["db_name"];
    var schema_name = constraint_list_json["schema_name"];
    var table_name = constraint_list_json["table_name"];

    var select_cmd = `SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_CATALOG = :1
                    AND TABLE_SCHEMA= :2
                    AND TABLE_NAME = :3
                    ;`;

    var select_out = executeStatement('binding_vars', select_cmd, [db_name, schema_name, table_name])

    var resOut = select_out.res_out;
    var select_out_array = [];

    while (resOut.next()) {
        var col_name = resOut.COLUMN_NAME;
        //col_name = "SUBSCRIPTION_TYPE_CODE";
        var column_length_cmd = `SELECT MAX(LENGTH(`+col_name+`)) as MAX_LENGTH from "`+db_name+`"."`+schema_name+`"."`+table_name+`";`;
        //return column_length_cmd;
        //return column_length_cmd;
        var column_length_out = executeStatement('', column_length_cmd, [])
        var column_length_res_out = column_length_out.res_out;
        column_length_res_out.next();
        var col_len = column_length_res_out.getColumnValue('MAX_LENGTH');
        //return col_len;
        var col_len =  col_name + ' : ' + col_len;
        select_out_array.push(col_len);
    }
    

    return select_out_array;

$$;
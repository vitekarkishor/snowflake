CREATE OR REPLACE PROCEDURE COM.COLLATE_TABLES_SP("TABLES_LIST" ARRAY)
RETURNS VARIANT
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
    var out ={};

    // Remember to capitalize variables input to the stored procedure definition
    for(i = 0; i < TABLES_LIST.length; i++){
        var table_res_rows =[];
        var original_table_name = TABLES_LIST[i];
        var new_table_name = original_table_name +'_COLLATE';
        //create table statement
        var create_table_cmd = `CREATE OR REPLACE TABLE `+new_table_name+` LIKE `+original_table_name+` COPY GRANTS;`
        var exec_res_out=executeStatement('', create_table_cmd, []);
        var resOut = exec_res_out.res_out;
        resOut.next();
        stmtOut = resOut.getColumnValueAsString(1);
        table_res_rows.push(stmtOut);


        //insert data into new table
        var insert_data_cmd = `INSERT INTO `+new_table_name+` SELECT * FROM `+original_table_name+`;`
        var exec_res_out=executeStatement('', insert_data_cmd, []);
        var resOut = exec_res_out.res_out;
        resOut.next();
        stmtOut = 'Number of rows inserted -->' + resOut.getColumnValueAsString(1);
        table_res_rows.push(stmtOut);

         //swap original table with new table
        var swap_tables_cmd = `ALTER TABLE `+new_table_name+` SWAP WITH `+original_table_name+`;`
        var exec_res_out=executeStatement('', swap_tables_cmd, []);
        var resOut = exec_res_out.res_out;
        resOut.next();
        stmtOut = 'Swap ' + resOut.getColumnValueAsString(1);
        table_res_rows.push(stmtOut);

        //drop table with new name
        var drop_table_cmd = `DROP TABLE `+new_table_name+`;`
        var exec_res_out=executeStatement('', drop_table_cmd, []);
        var resOut = exec_res_out.res_out;
        resOut.next();
        stmtOut = resOut.getColumnValueAsString(1);
        table_res_rows.push(stmtOut);

        out[original_table_name] = table_res_rows;
    }
return out;

$$;
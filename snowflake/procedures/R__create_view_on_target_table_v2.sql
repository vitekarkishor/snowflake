CREATE OR REPLACE PROCEDURE COM.CREATE_VIEW_ON_TARGET_TABLE_V2("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS OWNER
AS $$

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

    var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);
    var database_name = constraints_list_json["database_name"].toUpperCase();;
    var view_schema_name = constraints_list_json["view_schema_name"].toUpperCase();;
    var source_table_schema = constraints_list_json["source_table_schema"].toUpperCase();
	var table_list = constraints_list_json["source_table_list"];
    
    var results = []
    
    for(index in table_list){
	select_out_array= []
    var table_name = table_list[index].toUpperCase();
	var select_cmd = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_CATALOG ='"+database_name+ "' AND TABLE_SCHEMA = '" +source_table_schema+"' AND TABLE_NAME ='"+table_name+"' order by ORDINAL_POSITION asc ;"
	
	
	var select_cmd_val = executeStatement('', select_cmd, []);
	var final_stat = select_cmd_val.res_out;
	var select_out_array = [];
	
	while (final_stat.next()) {
		var col_name = final_stat.getColumnValue(1);
		select_out_array.push(col_name);
	}
	
	
	

     var view_ddl = "CREATE OR REPLACE VIEW " + database_name+"."+view_schema_name+"."+table_name+"_VW" + " AS SELECT "+  select_out_array.toString() +"  FROM "+source_table_schema+"."+table_name;
     var select_out = executeStatement('binding_vars', view_ddl, [database_name,source_table_schema,view_schema_name,table_name]);
     var resOut = select_out.res_out;
     resOut.next();
	 results.push(resOut.getColumnValue(1));
    }
	return results;
$$;
CREATE OR REPLACE PROCEDURE COM.GENERIC_REC_AGG_STATISTICS_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try{

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
	    
        var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
        var process_name = constraint_list_json["process_name"].toUpperCase();
		
		if('stat_type' in constraint_list_json && constraint_list_json["stat_type"].toUpperCase() == 'MANUAL'){
		    var etl_name = process_name;
			var schema_name = constraint_list_json["schema_name"].toUpperCase();
			var table_name = constraint_list_json["table_name"].toUpperCase();
			var field_name = constraint_list_json["field_name"].toUpperCase();
			var agg_date_col = constraint_list_json["agg_date_col"].toUpperCase();
			var agg_date_col_format = constraint_list_json["agg_date_col_format"].toUpperCase();
			
			tbl_list_qry = `select :1 ETL_NAME, :2 schema_name, :3 table_name, :4 field_name, :5 AGG_DATE_COL, :6 AGG_DATE_COL_FORMAT ;`;
            tbl_list_out = executeStatement('binding_vars', tbl_list_qry, [etl_name,schema_name,table_name,field_name,agg_date_col,agg_date_col_format]);
            tbl_list_out = tbl_list_out.res_out;
			
		}
		else{
		    tbl_list_qry = `select ETL_NAME, schema_name, table_name, field_name, AGG_DATE_COL, AGG_DATE_COL_FORMAT from STATISTICS.AGG_STATISTICS_CONFIG where etl_name = :1  and field_type = 'NUMERIC' and STATUS = 'ACTIVE' ;`;
            tbl_list_out = executeStatement('binding_vars', tbl_list_qry, [process_name]);
            tbl_list_out = tbl_list_out.res_out;
		}

	    

        	    
        while (tbl_list_out.next()) {
		    var etl_name = tbl_list_out.ETL_NAME;
            var schema_name = tbl_list_out.SCHEMA_NAME;
            var table_name = tbl_list_out.TABLE_NAME;
            var field_name = tbl_list_out.FIELD_NAME;
			var agg_date_col = tbl_list_out.AGG_DATE_COL;
			var agg_date_col_format = tbl_list_out.AGG_DATE_COL_FORMAT;
			if( agg_date_col_format == null || agg_date_col_format == '){
                load_date_param = `to_date(` + agg_date_col + `) AS AGG_KEY_CALC`;
                agg_date_col_format = 'YYYY-MM-DD';
            }	
            else{
			    load_date_param = `try_to_date(` + agg_date_col + `::varchar, '` + agg_date_col_format + `') AS AGG_KEY_CALC`;
            }			
   

			if( field_name == null || field_name == '){
                                var stat_cmd = `INSERT INTO STATISTICS.REC_AGG_STATISTICS
                                SELECT 
                                :1 AS ETL_NAME,
                                :2 AS SCHEMA_NAME,
                                :3 AS TABLE_NAME,
                                ` + load_date_param + `,
                                COUNT(*) AS ROWS_COUNT,
                                null COLUMN_NAME,
                                null,
                                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS INSERT_DATE_TIME
                                FROM ` + schema_name + `.` + table_name + ` 
                                WHERE ` + agg_date_col + ` >= to_varchar(dateadd(day, -14, current_date()),'`+agg_date_col_format+`')
                                group by AGG_KEY_CALC;`;

            }	
            else{
                var stat_cmd = `INSERT INTO STATISTICS.REC_AGG_STATISTICS
                    SELECT 
					:1 AS ETL_NAME,
					:2 AS SCHEMA_NAME,
                    :3 AS TABLE_NAME,
					` + load_date_param + `,
					COUNT(*) AS ROWS_COUNT,
					:4 AS COLUMN_NAME,
					sum( ` + field_name + `),
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS INSERT_DATE_TIME
                    FROM ` + schema_name + `.` + table_name + ` 
                    WHERE ` + agg_date_col + ` >= to_varchar(dateadd(day, -14, current_date()) ,'`+agg_date_col_format+`')
					group by AGG_KEY_CALC;`;

            }			

                //return stat_cmd;
                var binding_vars =[etl_name,schema_name,table_name,field_name,agg_date_col,agg_date_col_format];
                executeStatement('binding_vars', stat_cmd, binding_vars);
            
        }
        return 'Success'; 
	}
    catch (err) {
    return 'Error--->'+ err.message ;
}	
$$;
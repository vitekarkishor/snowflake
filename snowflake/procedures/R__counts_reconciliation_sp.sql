CREATE OR REPLACE PROCEDURE COM.COUNTS_RECONCILIATION_SP("EXECUTION_ID" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARIANT
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
try
{

   //---common function to execute the statement
   function executeStatement(exec_type, query_cmd, binding_vars)
   {

      if (exec_type == 'binding_vars')
      {
         var query_stmt = snowflake.createStatement(
         {
            sqlText: query_cmd,
            binds: binding_vars
         });
      }
      else
      {
         var query_stmt = snowflake.createStatement(
         {
            sqlText: query_cmd
         });
      }
      var res_out = query_stmt.execute();
      var query_id = query_stmt.getQueryId();
      return {
         res_out,
         query_id
      };
   }


   //---FETCH json AND other required variables
   var constraints_list_json = JSON.parse(CONSTRAINTS_LIST);

   
   var table_name = constraints_list_json["table_name"].toUpperCase();		

   var executed_sp = constraints_list_json["executed_sp"].toUpperCase();
   var s3_path = constraints_list_json["s3_path"];
   var partition_format = constraints_list_json["partition_format"];
   var no_of_months = constraints_list_json["no_of_months"];
   var partition_col_name = constraints_list_json["partition_col_name"];
   var table_schema = constraints_list_json["table_schema"];
   var external_stage_name = constraints_list_json["external_stage_name"].toUpperCase();
   var partition_format = constraints_list_json["partition_format"];
   var start_date = constraints_list_json["start_date"];
   var star_date_0 = start_date.split('-')
   star_date_0 =  [star_date_0[0],star_date_0[1]].join('-'); 
   
   if (partition_format ==="yyyy-mm-dd"){
	   var	sql_query = "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[0]+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+star_date_0;
	   var inc = 1;
	   while (inc < no_of_months)
	   {
		 var date1 = new Date(start_date);
		 date1.setMonth(date1.getMonth() + inc);
		 var new_date = date1.toISOString().split('-');
		 var new_date_1 = [new_date[0],new_date[1]].join('-')
		 sql_query += ' UNION ALL ';
		 sql_query += "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+new_date_1;
		 inc++;
		 
	   }
	   
	   var sql_query_final = "insert into COM.Counts_Reconciliation (select 'S3','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+"     , count(*) as count from("+sql_query+") ext_dataset  group by 3 order by 3 desc);"
 
	   var sql_table_query = "insert into COM.Counts_Reconciliation( select 'SF','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+", count(*) as count from "+table_schema+"."+table_name+" group by "+partition_col_name+");";

	   //return  sql_query_final + '---'+sql_table_query;
	  }
  
   
   
   else if (partition_format ==="yyyy/mm"){
	   var star_date_0 = start_date.split('/');
	   var	sql_query = "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[0]+"') as "+partition_col_name[0]+" ,  COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[1]+"') as "+partition_col_name[1]+ " from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name[0]+"="+star_date_0[0]+"/"+partition_col_name[1]+"="+star_date_0[1];
	   var inc = 1;

	   while (inc < no_of_months)
	   {
		 start_date	= star_date_0[0]+'-'+star_date_0[1]+'-01'
		 var date1 = new Date(start_date);
		 date1.setMonth(date1.getMonth() + inc);
		 var new_date = date1.toISOString().split('-');
		 
		 sql_query += ' UNION ALL ';
		 sql_query += "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[0]+"') as "+partition_col_name[0]+" ,  COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[1]+"') as "+partition_col_name[1]+ " from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name[0]+"="+new_date[0]+"/"+partition_col_name[1]+"="+new_date[1];
		 inc++;
		 
	   }
   	
		var sql_query_final = "insert into COM.Counts_Reconciliation (select 'S3','"+table_schema+"."+table_name+"' as table_name,concat("+partition_col_name[0]+",'-',"+partition_col_name[1]+") as date ,count(*) as count  from( "+sql_query+") ext_dataset  group by date order by date desc);"
	
	
		var sql_table_query = "insert into COM.Counts_Reconciliation( select 'SF','"+table_schema+"."+table_name+"' as table_name,concat("+partition_col_name[0]+",'-',"+partition_col_name[1]+") as date  count(*) as count from "+table_schema+"."+table_name+" group by "+partition_col_name+");";

	   
	   }
	   
	else if (partition_format ==="yyyy-mm"){
	   var	sql_query = "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[0]+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+start_date;
	   var inc = 1;
	   while (inc < no_of_months)
	   {
		 newstart_date = start_date+'-01'			
		 var date1 = new Date(newstart_date);
		 date1.setMonth(date1.getMonth() + inc);
		 var new_date = date1.toISOString().split('-');
		 var new_date_1 = [new_date[0],new_date[1]].join('-')
		 sql_query += ' UNION ALL ';
		 sql_query += "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+new_date_1;
		 inc++;
		 
	   }
	   
	   var sql_query_final = "insert into COM.Counts_Reconciliation (select 'S3','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+"     , count(*) as count from("+sql_query+") ext_dataset  group by 3 order by 3 desc);"
 
	   var sql_table_query = "insert into COM.Counts_Reconciliation( select 'SF','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+", count(*) as count from "+table_schema+"."+table_name+" group by "+partition_col_name+");";

	   //return  sql_query_final + '---'+sql_table_query;
	  }
	  
	else if (partition_format ==="yyyymm"){
	   var	sql_query = "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name[0]+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+start_date;
	   var inc = 1;
	   while (inc < no_of_months)
	   {
		 newstart_date = start_date.slice(0,4)+'-'+start_date.slice(4,7)	
		 
		 var date1 = new Date(newstart_date);
		 
		 date1.setMonth(date1.getMonth() + inc);
		 var new_date = date1.toISOString().split('-');
		 var new_date_1 = [new_date[0],new_date[1]].join('')
		 sql_query += ' UNION ALL ';
		 sql_query += "select COM.EXTRACT_PARTITION_VALUE_SQL(METADATA$FILENAME,'"+partition_col_name+"') as "+partition_col_name+" from  @COM."+external_stage_name+"/"+s3_path+"/"+partition_col_name+"="+new_date_1;
		 inc++;
		 
	   }
	   
	   var sql_query_final = "insert into COM.Counts_Reconciliation (select 'S3','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+"     , count(*) as count from("+sql_query+") ext_dataset  group by 3 order by 3 desc);"
 
	   var sql_table_query = "insert into COM.Counts_Reconciliation( select 'SF','"+table_schema+"."+table_name+"' as table_name,"+partition_col_name+", count(*) as count from "+table_schema+"."+table_name+" group by "+partition_col_name+");";

	   //return  sql_query_final + '---'+sql_table_query;
	  }
  
  
   
   var exec_res_out_max_val = executeStatement('', sql_query_final, []);
   var final_stat = exec_res_out_max_val.res_out;
   final_stat.next();
   var s3_stat = 'Inserted records for S3 :'+final_stat.getColumnValue(1)
   
   
   
   var exec_res_out_max_val = executeStatement('', sql_table_query, []);
   var final_stat = exec_res_out_max_val.res_out;
   final_stat.next();
   var sf_stat = 'Inserted records for Sf :'+final_stat.getColumnValue(1)
   return  sql_query_final+s3_stat+sf_stat;

     

   
   //-----make started entry in the master audit log
   extra_info = JSON.stringify(
   {
      "status": "Started",
      "start_time": START_TIME
   });


   
}
catch (err)
{

   var last_query_cmd = `select last_query_id()`;
   var exec_res_out = executeStatement('', last_query_cmd, []);
   var last_query_res = exec_res_out.res_out;
   last_query_res.next();
   var error_query_id = last_query_res.getColumnValue(1);

   snowflake.execute(
   {
      sqlText: `ROLLBACK`
   });

   var message = err.message.replace(/\n/g, " ").replace(/'/g, " ").replace(/\r/g, " ").replace(/"/g, " ");

   if (err.stackTraceTxt)
   {
      stack_trace_txt = err.stackTraceTxt.replace(/\n/g, " ").replace(/'/g, " ").replace(/\r/g, " ").replace(/"/g, " ");
   }
   else
   {
      stack_trace_txt = '';
   }
   var dtls_after_exec = JSON.stringify(
   {
      "FAIL_CODE": err.code,
      "STATE": err.state,
      "MESSAGE": message,
      "STACK_TRACE": stack_trace_txt,
      "QUERY_ID": error_query_id,
      "EXECUTED_SP": executed_sp,
      "EXECUTION_STATUS": "FAILURE"
   });


   return 'Error -> Technical Error Occured ' + dtls_after_exec;
}
$$;
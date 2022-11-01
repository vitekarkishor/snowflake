CREATE OR REPLACE PROCEDURE COM.GENERIC_INSERT_MASTER_AUDIT_LOG_SP("EXECUTION_ID" VARCHAR(16777216), "ETL_NAME" VARCHAR(16777216), "ETL_TASK_NAME" VARCHAR(16777216), "EXECUTED_SP" VARCHAR(16777216), "STATUS" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "END_TIME" VARCHAR(16777216), "EXTRA_INFORMATION" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$
     
    if (STATUS != 'Started'){
        var cmd = `INSERT INTO COM.MASTER_AUDIT_LOG (EXECUTION_ID,ETL_NAME,ETL_TASK_NAME,EXECUTED_SP,STATUS,START_TIME,END_TIME,EXTRA_INFORMATION)
              SELECT` +
              ` '`+ EXECUTION_ID + `',`+
              `'` + ETL_NAME + `',`+
              `'` + ETL_TASK_NAME + `',`+
              `'` + EXECUTED_SP + `',`+
              `'` + STATUS + `',` + 
              `'` + START_TIME + `',`+
              `'` + END_TIME + `',`+
              `PARSE_JSON('` + EXTRA_INFORMATION + `');`;
    }
    else{
       var cmd = `INSERT INTO COM.MASTER_AUDIT_LOG (EXECUTION_ID,ETL_NAME,ETL_TASK_NAME,EXECUTED_SP,STATUS,START_TIME,EXTRA_INFORMATION)
            SELECT` +
            ` '`+ EXECUTION_ID + `',`+
            `'` + ETL_NAME + `',`+
            `'` + ETL_TASK_NAME + `',`+
            `'` + EXECUTED_SP + `',`+
            `'` + STATUS + `',` + 
            `'` + START_TIME + `',`+
            `PARSE_JSON('` + EXTRA_INFORMATION + `');`;
    }

    var query_stmt = snowflake.createStatement(
                {
                    sqlText: cmd
                }
    );
    query_stmt.execute();
    
    return 'Master Audit Log Updated Successfully';   
	
$$;
CREATE OR REPLACE PROCEDURE COM.GENERIC_INSERT_PROCESS_AUDIT_LOG_SP("EXECUTION_ID" VARCHAR(16777216), "ETL_NAME" VARCHAR(16777216), "ETL_TASK_NAME" VARCHAR(16777216), "EXECUTED_SP" VARCHAR(16777216), "STATUS" VARCHAR(16777216), "START_TIME" VARCHAR(16777216), "END_TIME" VARCHAR(16777216), "DTLS_AFTER_EXEC" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS $$

    DTLS_AFTER_EXEC = DTLS_AFTER_EXEC.replace(/\n/g, " ").replace(/\n/g, " ");
                
    var cmd = `INSERT INTO COM.PROCESS_AUDIT_LOG (EXECUTION_ID,ETL_NAME,ETL_TASK_NAME,EXECUTED_SP,STATUS,START_TIME,END_TIME,DETAILS_AFTER_EXEC)
              SELECT` +
              ` '`+ EXECUTION_ID + `',`+
              `'` + ETL_NAME + `',`+
              `'` + ETL_TASK_NAME + `',`+
              `'` + EXECUTED_SP + `',`+
              `'` + STATUS + `',` + 
              `'` + START_TIME + `',`+
              `'` + END_TIME + `',`+
              `PARSE_JSON('` + DTLS_AFTER_EXEC + `');`;

    var stmt = snowflake.createStatement(
        {
            sqlText: cmd
        }
    );
    stmt.execute();

    return 'Audit log Record is Inserted';

$$;
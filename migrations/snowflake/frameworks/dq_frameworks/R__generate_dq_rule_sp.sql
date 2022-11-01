CREATE OR REPLACE PROCEDURE COM."GENERATE_DQ_RULE_SP"("CONSTRAINTS_LIST" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS
$$

    var constraint_list_json = JSON.parse(CONSTRAINTS_LIST);
    var db_name = constraint_list_json["db_name"];
    var schema_name = constraint_list_json["schema_name"];
    var rule_name = constraint_list_json["rule_name"];
    var table_name = constraint_list_json["table_name"];
    var identifier_name = constraint_list_json["identifier_name"];
    var created_by = constraint_list_json["created_by"];

    var exec_query_for_length_chk_rule = `insert into "DEV_DB_FDM"."COM"."DQ_MAPPING_TABLE"(database_name,schema_name,table_name,column_name,dq_schema,dq_rule,identifier_name,created_by,created_date)
        select table_catalog,table_schema,table_name,column_name,'COM','length_chk_ud','`+identifier_name+`','`+created_by+`',current_date() from "DEV_DB_FDM"."INFORMATION_SCHEMA"."COLUMNS" 
        where table_catalog='`+db_name+`' and table_schema='`+schema_name+`' and LENGTH(CHARACTER_MAXIMUM_LENGTH) > 0`;

    var exec_query_for_not_null_chk_rule = `insert into "DEV_DB_FDM"."COM"."DQ_MAPPING_TABLE"(database_name,schema_name,table_name,column_name,dq_schema,dq_rule,identifier_name,created_by,created_date)
        select table_catalog,table_schema,table_name,column_name,'COM','not_null_chk_ud','`+identifier_name+`','`+created_by+`',current_date() from "DEV_DB_FDM"."INFORMATION_SCHEMA"."COLUMNS" 
        where table_catalog='`+db_name+`' and table_schema='`+schema_name+`' and IS_NULLABLE='NO'`;

    var add_condition_table_name = `and table_name='`+table_name+`';`;

    var final_query_to_exex = ``;

    if(rule_name=='length_chk_ud'){
        var final_query_to_exex = exec_query_for_length_chk_rule;
    }else if(rule_name=='not_null_chk_ud'){
        var final_query_to_exex = exec_query_for_not_null_chk_rule;
    }

    if(table_name.length>0){
        final_query_to_exex += add_condition_table_name
    }else{
        final_query_to_exex += `;`;
    }

    var query_stmt = snowflake.createStatement({sqlText: final_query_to_exex});

    query_stmt.execute();

    return 'Rules are generated in DQ mapping table.';

$$;

/*
CALLING STORED PROCEDURE

CALL "DEV_DB_FDM"."COM"."GENERATE_DQ_RULE_SP"('{
"db_name":"DEV_DB_FDM",
"schema_name":"COM",
"rule_name":"length_chk_ud",
"table_name":"",
"created_by":"",
"identifier_name":""
}'::VARCHAR);

*/
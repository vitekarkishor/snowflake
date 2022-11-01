CREATE OR REPLACE FUNCTION COM.NOT_NULL_CHK_UD("COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    if (COLNAME == null){
        return "false";
    }
    else if(COLNAME == 'null' || COLNAME == 'NULL' || COLNAME == 'Null'){
        return "false";
    }
    else{
        return "true";
    }
$$;
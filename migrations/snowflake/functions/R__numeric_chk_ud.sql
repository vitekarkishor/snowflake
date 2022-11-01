CREATE OR REPLACE FUNCTION COM.NUMERIC_CHK_UD("COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    if (isNaN(COLNAME)){
        return "false";
    }
    else if (COLNAME == ''){
        return "false";
    }else{
        return "true";
    }
$$;
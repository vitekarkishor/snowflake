CREATE OR REPLACE FUNCTION COM.DECIMAL_CHK_UD("COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$

    if(isNaN(COLNAME) == false){
        if(Number(COLNAME) % 1 != 0){
            return "true";
        }else{
            return "false";
        }
    }else{
        return "false";
    }

$$;
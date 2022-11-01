CREATE OR REPLACE FUNCTION COM.EMAIL_CHK_UD("COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    var mailformat = /^\w+([\.-]?\w+)*@\w+([\.-]?\w+)*(\.\w{2,3})+$/;
    if(COLNAME.match(mailformat)){
        return "true";
    }else{
        return "false";
    }
$$;
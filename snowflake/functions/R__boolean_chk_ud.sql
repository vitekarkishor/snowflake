CREATE OR REPLACE FUNCTION COM.BOOLEAN_CHK_UD("COLNAME" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$ 
    if(COLNAME=='true' || COLNAME=='false' || COLNAME=='TRUE' || COLNAME=='FALSE' || COLNAME=='YES' || COLNAME=='NO' || COLNAME=='Yes' || COLNAME=='No' || COLNAME=='yes' || COLNAME=='no' || COLNAME==0 || COLNAME==1){
         return "true";
     }else {
         return "false";
     }
$$;
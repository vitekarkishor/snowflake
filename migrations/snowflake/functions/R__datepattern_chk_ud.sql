CREATE OR REPLACE FUNCTION COM.DATEPATTERN_CHK_UD("DATE" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    // DATE FORMAT (yyyy-mm-dd | dd-mm-yyyy | yyyy-dd-mm | mm-dd-yyyy)
    var regex=new RegExp("([0-9]{4}[-](0[1-9]|1[0-2])[-]([0-2]{1}[0-9]{1}|3[0-1]{1})|([0-2]{1}[0-9]{1}|3[0-1]{1})[-](0[1-9]|1[0-2])[-][0-9]{4}|[0-9]{4}[-]([0-2]{1}[0-9]{1}|3[0-1]{1})[-](0[1-9]|1[0-2])|(0[1-9]|1[0-2])[-]([0-2]{1}[0-9]{1}|3[0-1]{1})[-][0-9]{4})");
    var dateOk=regex.test(DATE);
    var regex_2 = new RegExp("([0-9]{4}[/](0[1-9]|1[0-2])[/]([0-2]{1}[0-9]{1}|3[0-1]{1})|([0-2]{1}[0-9]{1}|3[0-1]{1})[/](0[1-9]|1[0-2])[/][0-9]{4}|[0-9]{4}[/]([0-2]{1}[0-9]{1}|3[0-1]{1})[/](0[1-9]|1[0-2])|(0[1-9]|1[0-2])[/]([0-2]{1}[0-9]{1}|3[0-1]{1})[/][0-9]{4})");
    var dateOk_2=regex_2.test(DATE);
    if(dateOk){
        return "true";
    }else if(dateOk_2){
        return "true";
    }
    else{
        return "false";
}
$$;
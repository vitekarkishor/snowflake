CREATE OR REPLACE FUNCTION COM.EXTRACT_PARTITION_VALUE_JS("METADATAFILE_NAME" VARCHAR(16777216), "PARTITION_COL" VARCHAR(16777216))
RETURNS VARCHAR(16777216)
LANGUAGE JAVASCRIPT
AS $$
    try {        
        for (let file_element of METADATAFILE_NAME.split('/')) {
            if (file_element.includes(PARTITION_COL.toLowerCase().concat('='))){
                return file_element.split('=')[1];
            }                                
        }
    } catch (err) {
        return '';
    }
$$;
--This comment will be ignored
DELETE 
FROM stg_schema.stg_flat_file_01;

/*
As
will
this
one
*/

INSERT INTO stg_schema.stg_flat_file_01 (record_id, float_field_01, integer_field_01, date_field_01, timestamp_field_01, sourcefilename)
SELECT 
record_id, 
float_field_01, 
integer_field_01, 
date_field_01, 
timestamp_field_01, 
sourcefilename
FROM wrk_schema.wrk_flat_file_01;

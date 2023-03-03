--This comment will be ignored
DELETE 
FROM tgt_schema.stg_flat_file_01 TGT
WHERE EXISTS
(
	SELECT 1
	FROM stg_schema.stg_flat_file_01 STG
	WHERE STG.record_id = TGT.record_id
);

/*
As
will
this
one
*/
INSERT INTO tgt_schema.stg_flat_file_01 (record_id, float_field_01, integer_field_01, date_field_01, timestamp_field_01, sourcefilename)
SELECT 
record_id, 
float_field_01, 
integer_field_01, 
date_field_01, 
timestamp_field_01, 
sourcefilename
FROM stg_schema.stg_flat_file_01;

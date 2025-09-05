
--- Null values analysis

select
	count(*) filter (where serial_number is null) as null_serial_number,
	count(*) filter (where list_year is null) as null_list_year,
	count(*) filter (where date_recorded is null) as null_date_recorded,
	count(*) filter (where town is null) as null_town,
	count(*) filter (where address is null) as null_address,
	count(*) filter (where assessed_value is null) as null_assessed_value,
	count(*) filter (where address is null) as null_address,
	count(*) filter (where sale_amount is null) as null_sale_amount,
	count(*) filter (where sales_ratio is null) as null_sales_ratio,
	count(*) filter (where property_type is null) as null_property_type,
	count(*) filter (where residencial_type is null) as null_residencial_type,
	count(*) filter (where non_use_code is null) as null_non_use_code,
	count(*) filter (where assessor_remarks is null) as null_assessor_remarks,
	count(*) filter (where opm_remarks is null) as null_opm_remarks,
	count(*) filter (where location is null) as null_location
from high_roles.stage_table;

--- 2 date_recorded missing
select
	*
from high_roles.stage_table
where date_recorded is null;

--- these 2 records don't have information. Maybe under construction, or other factors.
delete from high_roles.stage_table 
where date_recorded is null;

--- we don't accept null town and address values, because this form our unique property identifier
DELETE FROM high_roles.stage_table
WHERE address IS NULL;




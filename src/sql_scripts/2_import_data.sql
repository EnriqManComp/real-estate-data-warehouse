--- Import data
create temp table tmp_import as 
select *
from high_roles.raw_table
with no data;

alter table tmp_import
drop column record_id;


copy tmp_import
from 'D:/Enrique/Portfolio/real-state-warehouse-management/data/Real_Estate_Sales_2001-2023_GL_20250901.csv'
with (
	format csv,
	header true,
	delimiter ','
);

select *
from tmp_import
limit 5;

insert into high_roles.raw_table (
	serial_number,
	list_year,
	date_recorded,
	town,
	address,
	assessed_value,
	sale_amount,
	sales_ratio,
	property_type,
	residencial_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
	)
select 
	serial_number,
	list_year,
	date_recorded,
	town,
	address,
	assessed_value,
	sale_amount,
	sales_ratio,
	property_type,
	residencial_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
from tmp_import;

select *
from high_roles.raw_table
limit 5;

insert into high_roles.stage_table (
	serial_number,
	list_year,
	date_recorded,
	town,
	address,
	assessed_value,
	sale_amount,
	sales_ratio,
	property_type,
	residencial_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
	)
select 
	serial_number,
	list_year,
	date_recorded,
	town,
	address,
	assessed_value,
	sale_amount,
	sales_ratio,
	property_type,
	residencial_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
from tmp_import;

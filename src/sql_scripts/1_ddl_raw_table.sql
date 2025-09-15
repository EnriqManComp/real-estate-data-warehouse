--- Create schema
create schema if not exists high_roles;

--- Create raw table
create table if not exists high_roles.raw_table (
	record_id serial primary key,
	serial_number bigint,
	list_year int,
	date_recorded text,
	town text,
	address text,
	assessed_value text,
	sale_amount decimal,
	sales_ratio text,
	property_type text,
	residencial_type text,
	non_use_code text,
	assessor_remarks text,
	opm_remarks text,
	location text
);

--- stage table to keep raw data as is.
 
create table if not exists high_roles.stage_table (
	record_id serial primary key,
	serial_number bigint,
	list_year int,
	date_recorded text,
	town text,
	address text,
	assessed_value text,
	sale_amount decimal,
	sales_ratio text,
	property_type text,
	residencial_type text,
	non_use_code text,
	assessor_remarks text,
	opm_remarks text,
	location text
);


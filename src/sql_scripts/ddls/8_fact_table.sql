--- creating fact table

--- DDL for fact table
create table if not exists high_roles.fact_table (
	date_recorded date,
	serial_number bigint, --- real estate agency, company, etc id
	property_id bigint,
	median_sales decimal, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio decimal,
	FOREIGN KEY (serial_number) REFERENCES high_roles.agent_dim(serial_number) --- foreign key to agent_dim
);


--- creating fact table

--- DDL for fact table
create table if not exists high_roles.fact_table (
	date_recorded date,
	serial_number bigint, --- real estate agency, company, etc id
	median_sales decimal, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio decimal,
	FOREIGN KEY (serial_number) REFERENCES high_roles.agent_dim(serial_number) --- foreign key to agent_dim
);

--- populate fact table
insert into high_roles.fact_table (
	date_recorded,
	serial_number, --- real estate agency, company, etc id
	median_sales, --- sales could have a lot of peaks, so median instead of mean.
	avg_sales_ratio	--- average sales ratio sale_amount/assessed_value
)
select
	daterecorded,
	serialnumber,
	percentile_cont(0.5) within group (order by saleamount) as median_sales,
	avg(salesratio)
from high_roles.stage_table
group by daterecorded, serialnumber;


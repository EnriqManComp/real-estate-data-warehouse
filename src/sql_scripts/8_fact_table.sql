select
	*
from high_roles.stage_table
order by record_id 
limit 5;



--- fact table
create table if not exists high_roles.fact_table (
	record_id bigserial primary key,
	date_recorded date,
	serial_number bigint, --- real estate agency, company, etc id
	median_sales decimal, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio decimal,
	FOREIGN KEY (serial_number) REFERENCES high_roles.agent_dim(serial_number)
);
--- populate fact table
insert into high_roles.fact_table (
	record_id,
	date_recorded,
	serial_number, --- real estate agency, company, etc id
	median_sales, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio	
)
select
	record_id,
	date_recorded,
	serial_number,
	percentile_cont(0.5) within group (order by sale_amount) as median_sales,
	avg(sales_ratio)
from high_roles.stage_table
group by record_id;

select
	*
from high_roles.fact_table
limit 5;

--- populate fact table
insert into high_roles.fact_table (
	date_recorded,
	serial_number, --- real estate agency, company, etc id
	median_sales, --- sales could have a lot of peaks, so median instead of mean.
	avg_sales_ratio	--- average sales ratio sale_amount/assessed_value
)
select
	date_recorded,
	serial_number,
	percentile_cont(0.5) within group (order by sale_amount) as median_sales,
	avg(sales_ratio)
from high_roles.stage_table
group by date_recorded, serial_number;


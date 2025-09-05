select
	*
from high_roles.stage_table
order by record_id 
limit 5;

--- fact table fields: serial_number
create table if not exists high_roles.fact_table (
	record_id bigserial primary key,
	date_recorded date,
	serial_number bigint, --- real estate agency, company, etc id
	property_id bigint NULL,    --- foreign key to property_dim
	median_sales decimal, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio decimal
);

insert into high_roles.fact_table (
	record_id,
	date_recorded,
	serial_number, --- real estate agency, company, etc id
	property_id,
	median_sales, --- sales could be a lot of peaks, so median instead of mean.
	avg_sales_ratio	
)
select
	record_id,
	date_recorded,
	serial_number,
	('x' || substr(md5(town || address), 1, 16))::BIT(64)::BIGINT AS property_id,
	percentile_cont(0.5) within group (order by sale_amount) as median_sales,
	avg(sales_ratio)
from high_roles.stage_table
group by record_id;

WITH dist_num_properties AS (
	SELECT
		date_recorded,
		serial_number,
		COUNT(DISTINCT property_id) as count_properties
	FROM high_roles.property_dim
	GROUP BY date_recorded, serial_number
)

UPDATE high_roles.fact_table f
set num_properties = dnp.count_properties
from dist_num_properties dnp
where f.serial_number = dnp.serial_number
and f.date_recorded = dnp.date_recorded;

select
	*
from high_roles.fact_table
order by record_id 
limit 5;


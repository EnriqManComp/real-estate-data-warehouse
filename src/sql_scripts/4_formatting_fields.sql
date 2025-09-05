select date_recorded
from high_roles.stage_table
ORDER BY date_recorded;

--- Format assessed_value field
alter table high_roles.stage_table
alter column assessed_value type decimal using(replace(assessed_value, ',', '')::decimal);

--- Format sales_ratio field
alter table high_roles.stage_table
alter column sales_ratio type decimal using(replace(sales_ratio, ',', '')::decimal);

--- Format date_recorded field
ALTER TABLE high_roles.stage_table 
ALTER COLUMN date_recorded TYPE DATE
USING to_date(date_recorded, 'MM/DD/YYYY');

UPDATE high_roles.stage_table
SET date_recorded = date_recorded  + INTERVAL '2000 years'
WHERE EXTRACT(YEAR FROM date_recorded) < 1900;

select * FROM high_roles.stage_table limit 5;


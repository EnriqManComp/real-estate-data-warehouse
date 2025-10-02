--- Formatting fields

--- Format assessed_value field to decimal and remove ',' in the values
alter table high_roles.stage_table
alter column assessed_value type decimal using(replace(assessed_value::text, ',', '')::decimal);

--- Format sales_ratio field to decimal and remove ',' in the values
alter table high_roles.stage_table
alter column sales_ratio type decimal using(replace(sales_ratio::text, ',', '')::decimal);

--- Format date_recorded field 
ALTER TABLE high_roles.stage_table 
ALTER COLUMN date_recorded TYPE DATE
USING to_date(date_recorded, 'YYYY-MM-DD"T"HH24:MI:SS.MS');

--- There are some typing error in date_recorded field, specifically in the year data. 
UPDATE high_roles.stage_table
SET date_recorded = date_recorded  + INTERVAL '2000 years'
WHERE EXTRACT(YEAR FROM date_recorded) < 1900;




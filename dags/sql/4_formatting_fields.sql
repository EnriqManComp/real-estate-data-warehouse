--- Formatting fields

--- Format assessed_value field to decimal and remove ',' in the values
alter table high_roles.stage_table
alter column assessedvalue type decimal using(replace(assessedvalue::text, ',', '')::decimal);

--- Format sales_ratio field to decimal and remove ',' in the values
alter table high_roles.stage_table
alter column salesratio type decimal using(replace(salesratio::text, ',', '')::decimal);

--- Format date_recorded field 
ALTER TABLE high_roles.stage_table 
ALTER COLUMN daterecorded TYPE DATE
USING to_date(daterecorded, 'YYYY-MM-DD"T"HH24:MI:SS.MS');

--- There are some typing error in date_recorded field, specifically in the year data. 
UPDATE high_roles.stage_table
SET daterecorded = daterecorded  + INTERVAL '2000 years'
WHERE EXTRACT(YEAR FROM daterecorded) < 1900;




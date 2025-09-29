
--- these 2 records don't have information. Maybe under construction, or other factors.
DELETE FROM high_roles.stage_table 
where date_recorded is null;

--- we don't accept null town and address values, because this form our unique property identifier
DELETE FROM high_roles.stage_table
WHERE address IS NULL;

DELETE FROM high_roles.stage_table
WHERE town IS NULL;





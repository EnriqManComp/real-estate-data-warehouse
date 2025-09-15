--- Create the property dimension
--- The data here is going to be scd type 1, so each property store 1 time

--- DDL for the property_dim table
CREATE TABLE IF NOT EXISTS high_roles.property_dim (
	property_id BIGINT PRIMARY KEY,
	town TEXT,
	address TEXT,
	property_type TEXT,
	non_use_code TEXT,
	assessor_remarks TEXT,
	opm_remarks TEXT,
	location TEXT
);

--- populate the property_dim table
INSERT INTO high_roles.property_dim (
	property_id,
	town,
	address,
	property_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
	)
SELECT 
	DISTINCT ('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id, --- create an unique identifier for each property
	town,
	address,
	property_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
FROM high_roles.stage_table
WHERE is_last_update = true;

SELECT *
FROM high_roles.property_dim
LIMIT 5;

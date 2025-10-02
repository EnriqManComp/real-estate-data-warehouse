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


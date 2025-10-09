-- DDL agent_property_dimension
CREATE TABLE IF NOT EXISTS danalyst_roles.agent_property_dim (
	serial_number BIGINT,
	num_properties INT, -- how many properties belong to the serial number
	principal_town TEXT, -- what is the most frequent town where this serial number sold properties.
	secondary_town TEXT, -- what is the second most frequent town where this serial number sold properties.
	PRIMARY KEY (serial_number, property_id) 
);
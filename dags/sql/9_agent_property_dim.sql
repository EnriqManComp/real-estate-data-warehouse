--- create agent_property_dim

--- DDL for agent_property_dim
CREATE TABLE IF NOT EXISTS high_roles.agent_property_dim (
	serial_number BIGINT,
	property_id BIGINT,
	PRIMARY KEY (serial_number, property_id),
	FOREIGN KEY (property_id) REFERENCES high_roles.property_dim(property_id), --- foreign key to property_dim
	FOREIGN KEY (serial_number) REFERENCES high_roles.agent_dim(serial_number) --- foreign key to agent_dim
);

--- populate agent_property_dim
INSERT INTO high_roles.agent_property_dim (
	serial_number,
	property_id
)
SELECT 
	DISTINCT serialnumber,
	('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id
FROM high_roles.stage_table
GROUP BY serialnumber, town, address;




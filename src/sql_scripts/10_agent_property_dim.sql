--- create property_dim

CREATE TABLE IF NOT EXISTS high_roles.agent_property_dim (
	serial_number BIGINT,
	property_id BIGINT,
	PRIMARY KEY (serial_number, property_id),
	FOREIGN KEY (property_id) REFERENCES high_roles.property_dim(property_id),
	FOREIGN KEY (serial_number) REFERENCES high_roles.agent_dim(serial_number) 
)

--- populate agent_property_dim
INSERT INTO high_roles.agent_property_dim (
	serial_number,
	property_id
)
SELECT 
	DISTINCT serial_number,
	('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id
FROM high_roles.stage_table
GROUP BY serial_number, town, address;

SELECT *
FROM high_roles.agent_property_dim
LIMIT 5;


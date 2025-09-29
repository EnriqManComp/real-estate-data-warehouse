--- create agent_property_dim
--- populate agent_property_dim
INSERT INTO high_roles.agent_property_dim (
	serial_number,
	property_id
)
SELECT 
	DISTINCT serial_number,
	('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id
FROM high_roles.stage_table
GROUP BY serial_number, town, address
ON CONFLICT (serial_number, property_id) DO NOTHING;




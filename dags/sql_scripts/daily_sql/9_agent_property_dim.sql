--- create agent_property_dim
--- populate agent_property_dim
INSERT INTO high_roles.agent_property_dim (
	serial_number,
	property_id,
	date_recorded
)
SELECT 
	DISTINCT serial_number,
	property_id,
	date_recorded
FROM high_roles.stage_table
GROUP BY serial_number, property_id, date_recorded
ON CONFLICT (serial_number, property_id) DO NOTHING;






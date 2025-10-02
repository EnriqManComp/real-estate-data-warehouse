--- populate agent_dim with unique serial_number of realtor agents
INSERT INTO high_roles.agent_dim (serial_number)
SELECT
	DISTINCT serial_number
FROM high_roles.stage_table
ON CONFLICT (serial_number) DO NOTHING;



--- create agent_dim
CREATE TABLE IF NOT EXISTS high_roles.agent_dim (
	serial_number BIGINT PRIMARY KEY
);

--- populate agent_dim with unique serial_number of realtor agents
INSERT INTO high_roles.agent_dim (serial_number)
SELECT
	DISTINCT serial_number
FROM high_roles.stage_table;

SELECT *
FROM high_roles.agent_dim
LIMIT 5;

--- adding name
ALTER TABLE high_roles.agent_dim 
ADD COLUMN name TEXT DEFAULT NULL;

--- adding main location
ALTER TABLE high_roles.agent_dim 
ADD COLUMN main_location TEXT DEFAULT NULL;

--- adding type of realtor: company, individual, etc...
ALTER TABLE high_roles.agent_dim 
ADD COLUMN type_realtor TEXT DEFAULT NULL;

SELECT *
FROM high_roles.agent_dim
LIMIT 5;

--- create agent_dim

--- DDL for agent_dim 
CREATE TABLE IF NOT EXISTS high_roles.agent_dim (
	serial_number BIGINT PRIMARY KEY
);

--- populate agent_dim with unique serial_number of realtor agents
INSERT INTO high_roles.agent_dim (serial_number)
SELECT
	DISTINCT serialnumber
FROM high_roles.stage_table;

--- Adding additional data for agent_dim (future use)
--- adding name field
ALTER TABLE high_roles.agent_dim 
ADD COLUMN name TEXT DEFAULT NULL;

--- adding main location
ALTER TABLE high_roles.agent_dim 
ADD COLUMN main_location TEXT DEFAULT NULL;

--- adding type of realtor: company, individual, etc...
ALTER TABLE high_roles.agent_dim 
ADD COLUMN type_realtor TEXT DEFAULT NULL;



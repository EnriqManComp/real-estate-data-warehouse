--- Create fact table
CREATE TABLE IF NOT EXISTS danalyst_roles.fact_table AS
SELECT
	*
FROM high_roles.fact_table;

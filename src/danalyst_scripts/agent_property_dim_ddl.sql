-- DDL agent_property_dimension
CREATE TABLE IF NOT EXISTS danalyst_roles.agent_property_dim (
	serial_number BIGINT,
	property_id BIGINT,
	how_long_owned INT, -- how long the property belong to the serial_number
	profit_margin_change_ratio DECIMAL, -- change ratio compared to previous sales_ratio
	assessed_value_change_ratio DECIMAL, -- change ratio compared to previous assessed value 
	sale_amount_change_ratio DECIMAL, -- change ratio compared to previous sales amount
	PRIMARY KEY (serial_number, property_id) 
);
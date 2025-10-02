--- create sales_dim

-- create a property attribute list for sales
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 
        FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = 'property_attrib' 
        AND n.nspname = 'high_roles'
    ) THEN
        CREATE TYPE high_roles.property_attrib AS (
            assessed_value DECIMAL,
            sale_amount DECIMAL,
            sales_ratio DECIMAL
        );
    END IF;
END $$;

-- Sale Dimension with Type 2 SCD
--- Rules to add new records:
--- 	- If the property keep the same sale value and the same realtor over time, the record will keep.
--- 	- If the property change the realtor or change any of the sale fields new record and update end_date field on the previous record.
 
CREATE TABLE IF NOT EXISTS high_roles.sales_dim (
    property_id BIGINT,
    initial_date DATE NOT NULL,
    end_date DATE NOT NULL,
    serial_number BIGINT,
    
    -- attributes for the sale
    sales_info high_roles.property_attrib[],  -- list for accumulate the changes of sales over time (same sale same interval, different sale new record)
    -- tracking columns
    is_change_agency BOOLEAN,   -- if the property change of agency (serial_number id)
    is_positive_ratio BOOLEAN,  -- if the property increase based on sales ratio compared with last value
    is_negative_ratio BOOLEAN,	-- if the property decrease based on sales ratio compared with last value
    is_not_ready BOOLEAN,       -- if there are not sale information is not ready to sale.

    -- PK: allows multiple versions of the same (agent, property) across time
    PRIMARY KEY (property_id, initial_date, end_date, serial_number),

    -- FK to agent_property_dim composite key
    FOREIGN KEY (property_id) REFERENCES high_roles.property_dim(property_id) --- foreign key to property_dim
);


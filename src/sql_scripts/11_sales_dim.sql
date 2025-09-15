-- property_id attribute list
CREATE TYPE property_attrib AS (
	assessed_value DECIMAL,
    sale_amount DECIMAL,
    sales_ratio DECIMAL
);

-- Sale Dimension with Type 2 SCD
--- Rules to add new records:
--- 	- If the property keep the same sale value and the same realtor over time, the record will keep.
--- 	- If the property change the realtor or change any of the sale fields new record and update end_date field on the previous record.
 
CREATE TABLE IF NOT EXISTS high_roles.sale_dim (
    property_id BIGINT,
    initial_date DATE NOT NULL,
    end_date DATE NOT NULL,
    serial_number BIGINT,
    
    -- attributes for the sale
    sales_info property_attrib[],  -- list for accumulate the changes of sales over time (same sale same interval, different sale new record)
    -- tracking columns
    is_change_agency BOOLEAN,   -- if the property change of agency (serial_number id)
    is_positive_ratio BOOLEAN,  -- if the property increase based on sales ratio compared with last value
    is_negative_ratio BOOLEAN,	-- if the property decrease based on sales ratio compared with last value
    is_not_ready BOOLEAN,       -- if there are not sale information is not ready to sale.

    -- PK: allows multiple versions of the same (agent, property) across time
    PRIMARY KEY (property_id, initial_date, end_date, serial_number),

    -- FK to agent_property_dim composite key
    FOREIGN KEY (property_id) REFERENCES high_roles.property_dim(property_id)
);

SELECT
	('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id,
	*
FROM high_roles.stage_table
WHERE ('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT = 5699741060725601054;


--------------------------------------------------------------------

DO $$
DECLARE
    _date DATE;
BEGIN
    FOR _date IN
        SELECT DISTINCT date_recorded
        FROM high_roles.stage_table
        ORDER BY date_recorded
    LOOP
        -- Compute combined info once
        WITH combine AS (
            SELECT
                COALESCE(td.property_id, yd.property_id) AS property_id,
                COALESCE(td.date_recorded, _date) AS initial_date,
                DATE '9999-12-31' AS end_date,
				td.serial_number,
                ARRAY[ROW(
                    td.assessed_value,
                    td.sale_amount,
                    td.sales_ratio
                )::property_attrib] AS sales_info,
                CASE
                    WHEN yd.serial_number IS NULL THEN FALSE
                    WHEN td.serial_number <> yd.serial_number THEN TRUE
                    ELSE FALSE
                END AS is_change_agency,
                CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio > (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_positive_ratio,
                CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio < (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_negative_ratio,
                CASE
                    WHEN td.assessed_value IS NULL
                      OR td.sale_amount IS NULL
                      OR td.sales_ratio IS NULL
                    THEN TRUE ELSE FALSE
                END AS is_not_ready,
                CASE
                    WHEN yd.sales_info IS NULL THEN FALSE
                    WHEN td.serial_number IS DISTINCT FROM yd.serial_number
                      OR td.sales_ratio IS DISTINCT FROM (yd.sales_info[1]).sales_ratio
                      OR td.assessed_value IS DISTINCT FROM (yd.sales_info[1]).assessed_value
                      OR td.sale_amount IS DISTINCT FROM (yd.sales_info[1]).sale_amount
                    THEN TRUE ELSE FALSE
                END AS has_changed
            FROM (
                SELECT
                    serial_number,
                    ('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id,
                    date_recorded,
                    assessed_value,
                    sale_amount,
                    sales_ratio
                FROM high_roles.stage_table
                WHERE date_recorded = _date
            ) td
            FULL OUTER JOIN (
                SELECT *
                FROM high_roles.sale_dim
                WHERE end_date = DATE '9999-12-31'
            ) yd
            ON td.property_id = yd.property_id
        )
		UPDATE high_roles.sale_dim sd
	    SET end_date = (_date - INTERVAL '1 day')::DATE
	    FROM combine c
	    WHERE sd.property_id = c.property_id
	      AND sd.end_date = DATE '9999-12-31'
	      AND c.has_changed = TRUE;
		
		WITH combine AS (
            SELECT
                COALESCE(td.property_id, yd.property_id) AS property_id,
                COALESCE(td.date_recorded, _date) AS initial_date,
                DATE '9999-12-31' AS end_date,
				td.serial_number,
                ARRAY[ROW(
                    td.assessed_value,
                    td.sale_amount,
                    td.sales_ratio
                )::property_attrib] AS sales_info,
                CASE
                    WHEN yd.serial_number IS NULL THEN FALSE
                    WHEN td.serial_number <> yd.serial_number THEN TRUE
                    ELSE FALSE
                END AS is_change_agency,
                CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio > (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_positive_ratio,
                CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio < (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_negative_ratio,
                CASE
                    WHEN td.assessed_value IS NULL
                      OR td.sale_amount IS NULL
                      OR td.sales_ratio IS NULL
                    THEN TRUE ELSE FALSE
                END AS is_not_ready,
                CASE
                    WHEN yd.sales_info IS NULL THEN FALSE
                    WHEN td.serial_number IS DISTINCT FROM yd.serial_number
                      OR td.sales_ratio IS DISTINCT FROM (yd.sales_info[1]).sales_ratio
                      OR td.assessed_value IS DISTINCT FROM (yd.sales_info[1]).assessed_value
                      OR td.sale_amount IS DISTINCT FROM (yd.sales_info[1]).sale_amount
                    THEN TRUE ELSE FALSE
                END AS has_changed
            FROM (
                SELECT
                    serial_number,
                    ('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id,
                    date_recorded,
                    assessed_value,
                    sale_amount,
                    sales_ratio
                FROM high_roles.stage_table
                WHERE date_recorded = _date
            ) td
            FULL OUTER JOIN (
                SELECT *
                FROM high_roles.sale_dim
                WHERE end_date = DATE '9999-12-31'
            ) yd
            ON td.property_id = yd.property_id
        )
		INSERT INTO high_roles.sale_dim (
		    property_id,
		    initial_date,
		    end_date,
			serial_number,
		    sales_info,
		    is_change_agency,
		    is_positive_ratio,
		    is_negative_ratio,
		    is_not_ready
		)
		SELECT
		    c.property_id,
		    c.initial_date,
		    c.end_date,
			c.serial_number,
		    c.sales_info,
		    c.is_change_agency,
		    c.is_positive_ratio,
		    c.is_negative_ratio,
		    c.is_not_ready
		FROM combine c;
    END LOOP;
END $$;

SELECT
    serial_number AS st_serial_number,
    ('x' || substr(md5(town || address),1,32))::BIT(64)::BIGINT AS property_id,
    date_recorded,
    assessed_value,
    sale_amount,
    sales_ratio,
    town,
    address
FROM high_roles.stage_table
WHERE ('x' || substr(md5(town || address),1,32))::BIT(64)::BIGINT = 1762963146255659038;





SELECT *
FROM high_roles.sale_dim;

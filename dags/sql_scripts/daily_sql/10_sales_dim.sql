--------------------------------------------------------------------

DO $$
DECLARE
    _date DATE;
BEGIN
	--- loop only for available dates
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
                )::high_roles.property_attrib] AS sales_info,
				--- case if change of agency
                CASE
                    WHEN yd.serial_number IS NULL THEN FALSE
                    WHEN td.serial_number <> yd.serial_number THEN TRUE
                    ELSE FALSE
                END AS is_change_agency,
                --- case if sales_ratio is positive compared to last ratio
				CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio > (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_positive_ratio,
				--- case if sales_ratio is negative compared to last ratio
                CASE
                    WHEN (yd.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN td.sales_ratio < (yd.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_negative_ratio,
				--- case if the property is not ready to sale
                CASE
                    WHEN td.assessed_value IS NULL
                      OR td.sale_amount IS NULL
                      OR td.sales_ratio IS NULL
                    THEN TRUE ELSE FALSE
                END AS is_not_ready,
				--- track any change between last and current records
                CASE
                    WHEN yd.sales_info IS NULL THEN FALSE
                    WHEN td.serial_number IS DISTINCT FROM yd.serial_number
                      OR td.sales_ratio IS DISTINCT FROM (yd.sales_info[1]).sales_ratio
                      OR td.assessed_value IS DISTINCT FROM (yd.sales_info[1]).assessed_value
                      OR td.sale_amount IS DISTINCT FROM (yd.sales_info[1]).sale_amount
                    THEN TRUE ELSE FALSE
                END AS has_changed
			--- today or current data
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
            FULL OUTER JOIN ( --- all previous data in sales_dim table
                SELECT *
                FROM high_roles.sales_dim
                WHERE end_date = DATE '9999-12-31'
            ) yd
            ON td.property_id = yd.property_id
        )
		--- update all old records
		UPDATE high_roles.sales_dim sd
	    SET end_date = (_date - INTERVAL '1 day')::DATE
	    FROM combine c
	    WHERE sd.property_id = c.property_id
	      AND sd.end_date = DATE '9999-12-31'
	      AND c.has_changed = TRUE;

		--- Same as before, but populating the sales_dim
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
                )::high_roles.property_attrib] AS sales_info,
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
                FROM high_roles.sales_dim
                WHERE end_date = DATE '9999-12-31'
            ) yd
            ON td.property_id = yd.property_id
        )
		INSERT INTO high_roles.sales_dim (
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


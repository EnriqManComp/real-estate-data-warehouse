SELECT
	MIN(date_recorded ), --- 1999-04-05
	MAX(date_recorded )  --- 2024-10-02
FROM high_roles.stage_table;
--;- sales dimensional table with SCD Type 2

CREATE TABLE high_roles.sales_dim (
	initial_date DATE,
	end_date DATE,
	serial_number BIGINT,
	property_id BIGINT,
	assessed_value DECIMAL,
	sale_amount DECIMAL,
	sales_ratio DECIMAL,
	is_actual_owner BOOLEAN,
	change_representative BOOLEAN,
	change_assessed_value BOOLEAN DEFAULT FALSE,
	change_sale_amount BOOLEAN DEFAULT FALSE,
	PRIMARY KEY (initial_date, end_date, serial_number, property_id)
);

DO $$
DECLARE
	_start_date DATE := '1999-04-05'::DATE;
	_end_date DATE := '2001-10-02'::DATE;
BEGIN
	WHILE _start_date < _end_date LOOP
		
		WITH current_data AS (
			SELECT
				date_recorded AS initial_date,
				'9999-12-31'::DATE AS end_date,
				serial_number,
				('x' || substr(md5(town || address), 1, 16))::BIT(64)::BIGINT AS property_id,
				assessed_value,
				sale_amount,
				sales_ratio			
			FROM high_roles.stage_table
			WHERE date_recorded = _start_date
		)
		
		--- update old records when the same property show
			
		UPDATE high_roles.sales_dim sd
		SET end_date = cd.initial_date - INTERVAL '1 day',
			change_representative = (sd.serial_number <> cd.serial_number),
			is_actual_owner = (sd.serial_number = cd.serial_number)			
		FROM current_data cd
		WHERE sd.property_id = cd.property_id
			AND sd.end_date = '9999-12-31'::DATE;
		
		WITH current_data AS (
			SELECT
				date_recorded AS initial_date,
				'9999-12-31'::DATE AS end_date,
				serial_number,
				('x' || substr(md5(town || address), 1, 16))::BIT(64)::BIGINT AS property_id,
				assessed_value,
				sale_amount,
				sales_ratio
			FROM high_roles.stage_table
			WHERE date_recorded = _start_date
		),
			previous_data AS (
			SELECT
				initial_date,
				end_date,
				serial_number,
				property_id,
				assessed_value,
				sale_amount,
				sales_ratio,
				is_actual_owner,
				change_representative,
				change_assessed_value,
				change_sale_amount	
			FROM high_roles.sales_dim
			WHERE initial_date < _start_date
		)
		--- insert new data
			
		INSERT INTO high_roles.sales_dim (
			initial_date,
	        end_date,
	        serial_number,
	        property_id,
	        assessed_value,
	        sale_amount,
	        sales_ratio,
	        is_actual_owner,
	        change_representative,
	        change_assessed_value,
	        change_sale_amount
		)
		SELECT
			cd.initial_date,
	        cd.end_date,
	        cd.serial_number,
	        cd.property_id,
	        cd.assessed_value,
	        cd.sale_amount,
	        cd.sales_ratio,
	        TRUE AS is_actual_number,
	        FALSE AS change_representative,
	        (cd.assessed_value <> COALESCE(pd.assessed_value, cd.assessed_value))::BOOLEAN AS change_assessed_value,
	        (cd.sale_amount <> COALESCE(pd.sale_amount, cd.sale_amount))::BOOLEAN AS change_sale_amount
	    FROM current_data cd
	    LEFT JOIN previous_data pd
	    	ON cd.property_id = pd.property_id
				AND pd.end_date = '9999-12-31'::DATE;
		
		_start_date := _start_date + INTERVAL '1 day';
	END LOOP;
END $$;


	
SELECT
	*
FROM high_roles.sales_dim;

SELECT
	*
FROM high_roles.stage_table
WHERE date_recorded <= '2000-12-31'::DATE;


WITH agent_property_table AS (
	SELECT
		apd.*,
		EXTRACT(YEAR FROM date_recorded) AS year
	FROM high_roles.agent_dim ad
	LEFT JOIN high_roles.agent_property_dim apd
	ON ad.serial_number = apd.serial_number
),
	count_properties AS (
		SELECT
			*,
			COUNT(*) AS num_properties
		FROM agent_property_table
		GROUP BY serial_number, property_id, date_recorded, year
),
	add_town AS (
	SELECT
		cp.*,
		pd.town
	FROM count_properties cp
	INNER JOIN high_roles.property_dim pd
		ON cp.property_id = pd.property_id
),
	count_town AS (
	SELECT
		serial_number,
		year,
		town,
		COUNT(*) AS count_town
	FROM add_town
	GROUP BY serial_number, year, town
),
ranked_towns AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY serial_number, year ORDER BY count_town DESC) AS rnk -- rank each town by their frequency
    FROM count_town
),
	principal_secondary_town AS (
	SELECT
	    serial_number,
	    year,
	    CASE 
	        WHEN COUNT(*) FILTER (WHERE rnk = 1) = 1 THEN -- count the number of town with rank =1 
	            MAX(CASE WHEN rnk = 1 THEN town END) -- for rows where rank =1 its output is town name, otherwise NULL
	        ELSE 										  -- if there are more than 1 with rank =1 then NOT DEFINED YET
	            'NOT DEFINED YET'
	    END AS principal_town,
		CASE
			WHEN COUNT(*) FILTER (WHERE rnk = 2) = 1 THEN
				MAX(CASE WHEN rnk = 2 THEN town END)
			ELSE
				'NOT DEFINED YET'
		END AS secondary_town
	FROM ranked_towns
	GROUP BY serial_number, year
)
SELECT
	cp.serial_number,
	cp.num_properties,
	pst.principal_town,
	pst.secondary_town
FROM count_properties cp
LEFT JOIN principal_secondary_town pst
ON cp.serial_number = pst.serial_number;
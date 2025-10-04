from sqlalchemy import text

def insert_sales_dim(engine):
    """ Insert the new records into sales dimension """

    query = text("""
        WITH previous AS (
            SELECT
                *
            FROM high_roles.sales_dim
        ),
            current AS (
                SELECT
                    serial_number,
                    property_id,
                    date_recorded,
                    assessed_value,
                    sale_amount,
                    sales_ratio
                FROM high_roles.stage_table
        ),
            combine AS (
            SELECT
                cr.property_id,
                cr.date_recorded AS initial_date,
                DATE '9999-12-31' AS end_date,
                cr.serial_number,
                ARRAY[ROW(
                    cr.assessed_value,
                    cr.sale_amount,
                    cr.sales_ratio
                )::high_roles.property_attrib] AS sales_info,
                CASE
                    WHEN pr.serial_number IS NULL THEN FALSE
                    WHEN cr.serial_number <> pr.serial_number THEN TRUE
                    ELSE FALSE
                END AS is_change_agency,
                CASE
                    WHEN (pr.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN cr.sales_ratio > (pr.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_positive_ratio,
                CASE
                    WHEN (pr.sales_info[1]).sales_ratio IS NULL THEN FALSE
                    WHEN cr.sales_ratio < (pr.sales_info[1]).sales_ratio THEN TRUE
                    ELSE FALSE
                END AS is_negative_ratio,
                CASE
                    WHEN cr.assessed_value IS NULL
                    OR cr.sale_amount IS NULL
                    OR cr.sales_ratio IS NULL
                    THEN TRUE ELSE FALSE
                END AS is_not_ready	
                FROM current cr
                LEFT JOIN LATERAL (
                SELECT *
                FROM previous pr
                WHERE pr.property_id = cr.property_id
                ORDER BY pr.initial_date DESC
                LIMIT 1
            ) pr ON TRUE
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
        SELECT *
        FROM combine;
    """)
    with engine.begin() as conn:   # handles transaction & commit
        conn.execute(query)


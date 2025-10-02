from sqlalchemy import text

def update_sales_dim(engine, logic_date):
    """ Update the sales dimension when a property change """

    update_query = text("""
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
                    sales_ratio,
                    is_last_update
                FROM high_roles.stage_table
        ),
            combine AS (
                SELECT
                    pr.*
                FROM current cr
                INNER JOIN previous pr
                    ON cr.property_id = pr.property_id
            )
        UPDATE high_roles.sales_dim sd
        SET end_date = (CAST(:logic_date AS DATE) - INTERVAL '1 day')::DATE
        FROM combine c
            WHERE sd.property_id = c.property_id
            AND sd.end_date = DATE '9999-12-31';
    """)
    with engine.begin() as conn:   # handles transaction & commit
        conn.execute(update_query, {"logic_date": logic_date})


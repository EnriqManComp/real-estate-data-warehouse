from sqlalchemy import text

def update_same_day(engine, logic_date):
    """ Update the sales dimension when a property change """

    update_query = text("""
        WITH duplicates_same_day AS (
            SELECT
                *
            FROM high_roles.sales_dim
            WHERE initial_date = (:logic_date)::DATE
                AND property_id IN (
                    SELECT
                        property_id
                    FROM high_roles.sales_dim
                    GROUP BY property_id, initial_date, end_date
                    HAVING COUNT(*) > 1	
                )
        ),
        non_last_update AS (
            SELECT
                serial_number
            FROM high_roles.stage_table
            WHERE is_last_update = FALSE::BOOLEAN
        )
        UPDATE high_roles.sales_dim sd
        SET end_date = (:logic_date)::DATE
        FROM duplicates_same_day dsd
        INNER JOIN non_last_update nlu
            ON dsd.serial_number = nlu.serial_number
        WHERE sd.serial_number = dsd.serial_number
            AND sd.end_date = '9999-12-31'::DATE
            AND sd.property_id = dsd.property_id
            AND sd.initial_date = (:logic_date)::DATE;

    """)
    with engine.begin() as conn:   # handles transaction & commit
        conn.execute(update_query, {"logic_date": logic_date})


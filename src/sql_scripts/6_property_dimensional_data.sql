 
--- property dimensional table (This dimension only store the relation serial_number and the current property possesion, not track the changes over time)
--- SCD 1 Type (Each property only exist one time in this table)

CREATE TABLE IF NOT EXISTS high_roles.property_dim (
    property_id BIGINT PRIMARY KEY,
    date_recorded DATE NOT NULL,
    serial_number BIGINT NOT NULL,  -- agency id (current owner)
    town TEXT,
    address TEXT,
    property_type TEXT,
    residencial_type TEXT,
    non_use_code TEXT,
    assessor_remarks TEXT,
    opm_remarks TEXT,
    location TEXT
);

WITH property_info AS (
    SELECT DISTINCT
        date_recorded,
        serial_number,
        town,
        address,
        property_type,
        residencial_type,
        non_use_code,
        assessor_remarks,
        opm_remarks,
        location
    FROM high_roles.stage_table
    WHERE is_last_update = TRUE
      AND is_last_representative = TRUE
)

INSERT INTO high_roles.property_dim (
    property_id, date_recorded, serial_number, town, address,
    property_type, residencial_type, non_use_code, assessor_remarks,
    opm_remarks, location
)
SELECT
    ('x' || substr(md5(town || address), 1, 16))::bit(64)::bigint AS property_id,
    date_recorded,
    serial_number,
    town,
    address,
    property_type,
    residencial_type,
    non_use_code,
    assessor_remarks,
    opm_remarks,
    location
FROM property_info;

SELECT *
FROM high_roles.property_dim
WHERE property_id = 1310593460474930175
ORDER BY date_recorded;
--- 



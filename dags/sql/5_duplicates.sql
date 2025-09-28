--- duplicates analysis

-- Remove duplicates if the record was taking the same date, for the same serial_number, and the same property.
-- There are a lot of these pattern in the data without changes

WITH duplicates AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY daterecorded, serialnumber, town, address
            ORDER BY ctid
        ) AS rn
    FROM high_roles.stage_table
)
DELETE FROM high_roles.stage_table
WHERE ctid IN (
    SELECT ctid FROM duplicates WHERE rn > 1
);

-----------

--- There are many updates in different dates of the same property
--- So, I'm going to add a new boolean field is_last_update
--- This field track the updates over properties over time.
--- Also, I'm going to add is_last_representative, tracking the last owner.

--- Adding the two tracking flags

ALTER TABLE high_roles.stage_table
ADD COLUMN is_last_update BOOLEAN DEFAULT FALSE,
ADD COLUMN is_last_representative BOOLEAN DEFAULT FALSE;

--- is_last_update

WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY town, address
            ORDER BY daterecorded DESC
        ) AS rn
    FROM high_roles.stage_table
)
UPDATE high_roles.stage_table st
SET is_last_update = (r.rn = 1)
FROM ranked r
WHERE st.ctid = r.ctid;

--- is_last_representative
WITH ranked AS (
    SELECT
        ctid,
        ROW_NUMBER() OVER (
            PARTITION BY town, address
            ORDER BY daterecorded DESC
        ) AS rn
    FROM high_roles.stage_table
)
UPDATE high_roles.stage_table st
SET is_last_representative = (r.rn = 1)
FROM ranked r
WHERE st.ctid = r.ctid;





ALTER TABLE high_roles.stage_table
ADD COLUMN property_id BIGINT;

UPDATE high_roles.stage_table
SET property_id = ('x' || substr(md5(town || address), 1, 16))::bit(64)::bigint;

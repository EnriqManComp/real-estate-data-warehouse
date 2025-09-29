
--- populate the property_dim table
INSERT INTO high_roles.property_dim (
	property_id,
	town,
	address,
	property_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
	)
SELECT 
	DISTINCT ('x' || substr(md5(town || address),1,16))::BIT(64)::BIGINT AS property_id, --- create an unique identifier for each property
	town,
	address,
	property_type,
	non_use_code,
	assessor_remarks,
	opm_remarks,
	location
FROM high_roles.stage_table
WHERE is_last_update = true
ON CONFLICT (property_id) DO NOTHING;

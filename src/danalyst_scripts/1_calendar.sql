-- Create calendar for data analyst team
CREATE TABLE IF NOT EXISTS danalyst_roles.calendar AS
SELECT
	calendar_date, -- 1 day interval
	EXTRACT(YEAR FROM calendar_date) AS year, -- year
	EXTRACT(MONTH FROM calendar_date) AS month, --month
	EXTRACT(DAY FROM calendar_date) AS day, --day
	EXTRACT(WEEK FROM calendar_date) AS week_number, --week_number
	TO_CHAR(calendar_date, 'Day') AS day_of_week, --day of the week
	TO_CHAR(calendar_date, 'Month') AS month_name --month name
FROM GENERATE_SERIES(
	'2018-01-01'::DATE,
	'2025-12-31'::DATE,
	'1 day'::INTERVAL
) AS calendar_date;

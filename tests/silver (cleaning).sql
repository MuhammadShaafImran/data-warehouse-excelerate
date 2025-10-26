USE DataWarehouse_Excelerate;

-- Preview first few rows from all bronze tables for initial inspection
SELECT TOP(15) * FROM bronze.learner_opportunity_raw;
SELECT TOP(15) * FROM bronze.learner_raw;
SELECT TOP(15) * FROM bronze.marketing_23_24;
SELECT TOP(15) * FROM bronze.opportunity_raw;


-------------------------------------------------------------------
-- Cleaning bronze.cognito_raw2 -> silver.cognito_raw2
-------------------------------------------------------------------

-- Inspect raw data sample
SELECT TOP(15) * FROM bronze.cognito_raw2;

-- Check for duplicate user IDs
SELECT userid
FROM bronze.cognito_raw2
GROUP BY userid
HAVING COUNT(*) > 1;

-- Check for duplicate emails
SELECT email, COUNT(*) AS total_count
FROM bronze.cognito_raw2
GROUP BY email
HAVING COUNT(*) > 1;

-- Display users with duplicate emails (keep latest by modification date)
SELECT * 
FROM (
	SELECT *,
		ROW_NUMBER() OVER (PARTITION BY email ORDER BY userlastmodifieddate DESC) AS dup
	FROM bronze.cognito_raw2
) AS sub_table
WHERE dup = 1;

-- Count users with missing demographic data
SELECT COUNT(*) AS total_null 
FROM bronze.cognito_raw2 
WHERE gender IN ('NULL') 
  AND birthdate IN ('NULL') 
  AND city IN ('NULL') 
  AND zip IN ('NULL') 
  AND state IN ('NULL');

-- Check if duplicate users also exist in learner_raw
SELECT * 
FROM (
	SELECT c.*, l.*, 
		   ROW_NUMBER() OVER(PARTITION BY email ORDER BY userlastmodifieddate DESC) AS dup
	FROM bronze.cognito_raw2 AS c
	INNER JOIN bronze.learner_raw AS l 
		ON SUBSTRING(l.learner_id,9,LEN(l.learner_id)) = c.userid
) AS t
WHERE dup > 1;

-- Check gender value variations and frequency
SELECT gender, COUNT(*) 
FROM bronze.cognito_raw2
GROUP BY gender;

-- Review standardized gender values
SELECT DISTINCT 
	CASE 
		WHEN gender = 'NULL' THEN 'n/a'
		WHEN gender = 'Don%27t want to specify' THEN 'Other'
		ELSE gender
	END AS gender
FROM bronze.cognito_raw2;

-- Validate and clean birthdate column
SELECT birthdate 
FROM bronze.cognito_raw2
WHERE birthdate != TRIM(birthdate);

-- Calculate user ages (exclude 'NULL' values)
SELECT 
	CASE 
		WHEN birthdate != 'NULL' THEN DATEDIFF(YEAR, CAST(birthdate AS DATE), GETDATE())
		ELSE 0
	END AS age
FROM bronze.cognito_raw2;

-- Check for leading/trailing spaces in location fields
SELECT city, zip, state
FROM bronze.cognito_raw2
WHERE city != TRIM(city) OR zip != TRIM(zip) OR state != TRIM(state);

-- Verify distinct cleaned city names
SELECT DISTINCT LOWER(TRIM(city)) AS city
FROM bronze.cognito_raw2;

-- Identify city names that end with the word "city"
SELECT city 
FROM bronze.cognito_raw2
WHERE city LIKE '%city';

-- Check how many users with complete demographic info are enrolled
SELECT COUNT(*)
FROM bronze.cognito_raw2 AS c
INNER JOIN bronze.learner_opportunity_raw AS l 
	ON SUBSTRING(l.enrollment_id,9,LEN(l.enrollment_id)) = c.userid
WHERE c.gender != 'NULL' 
  AND c.birthdate != 'NULL'
  AND c.city != 'NULL'
  AND c.zip != 'NULL'
  AND c.state != 'NULL';


-------------------------------------------------------------------
-- Cleaning bronze.cohortraw -> silver.cohortraw
-------------------------------------------------------------------

-- Count total records
SELECT COUNT(*) FROM bronze.cohortraw;

-- Check for duplicate cohort codes
SELECT COUNT(DISTINCT cohort_code) AS dis_cohort_code, COUNT(*) AS total_rows
FROM bronze.cohortraw;

-- Convert timestamps (milliseconds) to readable date format
SELECT DATEADD(SECOND, start_date / 1000, '1970-01-01') AS converted_date
FROM bronze.cohortraw;

-- Verify that end_date is after start_date
SELECT COUNT(*)
FROM silver.cohortraw
WHERE end_date < start_date;

-- Check distinct cohort sizes
SELECT DISTINCT size 
FROM bronze.cohortraw;


---------------------------------------------------------------------------------
-- Cleaning bronze.learner_opportunity_raw -> silver.learner_opportunity_raw
---------------------------------------------------------------------------------

-- Inspect raw data
SELECT * FROM bronze.learner_opportunity_raw;

-- Extract opportunity ID from learner_id
SELECT SUBSTRING(learner_id,13,LEN(learner_id)) AS opportunity_id
FROM bronze.learner_opportunity_raw;

-- Convert apply_date to proper datetime
SELECT CAST(REPLACE(REPLACE(apply_date, 'Z', ''), 'T', ' ') AS DATETIME2) AS converted_date
FROM bronze.learner_opportunity_raw
WHERE apply_date != 'NULL';

-- Check duplicate enrollment IDs
SELECT enrollment_id, COUNT(*) AS cnt
FROM bronze.learner_opportunity_raw
GROUP BY enrollment_id
HAVING COUNT(*) > 1;

-- List all distinct statuses
SELECT DISTINCT status
FROM bronze.learner_opportunity_raw;


---------------------------------------------------------------------------------
-- Cleaning bronze.learner_raw -> silver.learner_raw
---------------------------------------------------------------------------------

-- Inspect raw data
SELECT * FROM bronze.learner_raw;

-- Find duplicate learner IDs
SELECT learner_id
FROM bronze.learner_raw
GROUP BY learner_id
HAVING COUNT(*) > 1;

-- Check and clean up country names
SELECT country, COUNT(*) AS counts
FROM bronze.learner_raw
GROUP BY country
ORDER BY country;

-- Identify invalid or unknown countries not matching world_cities reference
SELECT country 
FROM (
	SELECT REPLACE(REPLACE(LOWER(country), '"', ''), 'd%27', '''') AS country
	FROM bronze.learner_raw
	WHERE country != 'null'
) AS t
WHERE country NOT IN (SELECT LOWER(country) FROM Helper.dbo.world_cities);

-- Attempt fuzzy match for unmatched countries
SELECT 
    s.learner_id,
    s.country AS original,
    w.country AS matched
FROM (
	SELECT * 
	FROM bronze.learner_raw
	WHERE country NOT IN (SELECT country FROM helper.dbo.world_cities)
) AS s
CROSS APPLY (
    SELECT TOP 1 w.country
    FROM Helper.dbo.world_cities AS w
    WHERE DIFFERENCE(s.country, w.country) >= 3
    ORDER BY DIFFERENCE(s.country, w.country) DESC
) AS w;

-- Review degree column for normalization
SELECT degree, COUNT(*) AS total_counts
FROM bronze.learner_raw
GROUP BY degree;

-- Standardize degree values
SELECT 
	CASE 
		WHEN degree = 'Parent of Student' THEN 'Not in Education'
		WHEN degree IS NULL THEN 'n/a'
		WHEN degree LIKE '%Student' THEN TRIM(REPLACE(degree,'Student',''))
		ELSE degree
	END AS cleaned_degree
FROM bronze.learner_raw;

-- Check institution formatting
SELECT LOWER(TRIM(institution)) AS institution, COUNT(*) AS total
FROM bronze.learner_raw
GROUP BY LOWER(TRIM(institution))
ORDER BY total DESC;

-- Identify invalid or missing institution names
SELECT institution
FROM bronze.learner_raw
WHERE LOWER(institution) IS NULL OR LEN(LOWER(institution)) = 1;

-- Review major column for null or invalid values
SELECT DISTINCT LOWER(major)
FROM bronze.learner_raw
WHERE LEN(major) = 1 OR major IS NULL;


---------------------------------------------------------------------------------
-- Cleaning bronze.opportunity_raw -> silver.opportunity_raw
---------------------------------------------------------------------------------

-- Inspect raw data
SELECT * FROM bronze.opportunity_raw;

-- Extract opportunity_id substring
SELECT SUBSTRING(opportunity_id,13,LEN(opportunity_id))
FROM bronze.opportunity_raw;

-- Review unique opportunity categories
SELECT DISTINCT category
FROM bronze.opportunity_raw;

-- Check for duplicate opportunity codes
SELECT COUNT(DISTINCT opportunity_code), COUNT(opportunity_code)
FROM bronze.opportunity_raw;

-- Expand JSON from tracking_question column for validation
SELECT
    t.opportunity_id,
    j.[code],
    j.[question],
    j.[is_frozen],
    j.[ans_type],
    j.[is_required_for_badge_award]
FROM bronze.opportunity_raw t
CROSS APPLY OPENJSON(
    '[' + REPLACE(CAST(t.tracking_question AS NVARCHAR(MAX)), '""', '"') + ']'
)
WITH (
    [code] NVARCHAR(100),
    [question] NVARCHAR(MAX),
    [is_frozen] NVARCHAR(10),
    [ans_type] NVARCHAR(50),
    [is_required_for_badge_award] NVARCHAR(10)
) AS j
WHERE t.tracking_question IS NOT NULL;


---------------------------------------------------------------------------------
-- Cleaning bronze.marketing_23_24 -> silver.marketing_23_24
---------------------------------------------------------------------------------

-- Inspect marketing data
SELECT * FROM bronze.marketing_23_24;

-- Check unique ad accounts
SELECT DISTINCT ad_account_name
FROM bronze.marketing_23_24;

-- Count rows missing campaign_name
SELECT COUNT(*) AS missing_campaign_name
FROM bronze.marketing_23_24
WHERE campaign_name IS NULL;

-- Review distinct delivery statuses
SELECT DISTINCT delivery_status
FROM bronze.marketing_23_24;

-- Check for negative metric values (invalid)
SELECT * 
FROM bronze.marketing_23_24
WHERE reach < 0 
   OR outbound_click < 0 
   OR results < 0 
   OR cost_per_result < 0 
   OR amount_spend_aed < 0 
   OR cpc < 0;

-- Review distinct result types
SELECT DISTINCT result_type
FROM bronze.marketing_23_24;

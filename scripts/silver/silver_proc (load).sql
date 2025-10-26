CREATE OR ALTER PROCEDURE silver.load_silver
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME = GETDATE(),
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    PRINT('---------------------------------------------------');
    PRINT('Starting Silver Load Process');
    PRINT('Start Time: ' + CONVERT(VARCHAR(30), @start_time, 120));
    PRINT('---------------------------------------------------');

    BEGIN TRY

        -------------------------------------------------------------------
        -- silver.cognito_raw2
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.cognito_raw2 at ' + CONVERT(VARCHAR(30), @batch_start_time, 120));

        INSERT INTO silver.cognito_raw2 (userid, email, gender, create_date, last_modified_date, age, city, zip, state)
        SELECT 
            userid,
            email,
            CASE 
                WHEN gender = 'Don%27t want to specify' THEN 'Other'
                ELSE LOWER(gender)
            END AS gender,
            usercreatedate AS create_date,
            userlastmodifieddate AS last_modified_date,
            CASE 
                WHEN birthdate != 'NULL' THEN DATEDIFF(YEAR, CAST(birthdate AS DATE), GETDATE())
                ELSE 0
            END AS age,
            CASE 
                WHEN city != 'NULL' THEN LOWER(TRIM(city))
                ELSE city
            END AS city,
            CASE 
                WHEN zip != 'NULL' THEN LOWER(TRIM(zip))
                ELSE zip
            END AS zip,
            CASE 
                WHEN state != 'NULL' THEN LOWER(TRIM(state))
                ELSE state
            END AS state
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER(PARTITION BY email ORDER BY userlastmodifieddate DESC) AS last_user_values
            FROM bronze.cognito_raw2
        ) AS sub_table 
        WHERE last_user_values = 1 
          AND gender != 'NULL' 
          AND city != 'NULL' 
          AND zip != 'NULL' 
          AND state != 'NULL';

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.cognito_raw2 in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.cohortraw
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.cohortraw');

        INSERT INTO silver.cohortraw (cohort_code, start_date, end_date, size)
        SELECT 
            cohort_code,
            DATEADD(SECOND, start_date / 1000, '1970-01-01') AS start_date,
            DATEADD(SECOND, end_date / 1000, '1970-01-01') AS end_date,
            size
        FROM bronze.cohortraw;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.cohortraw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.learner_opportunity_raw
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.learner_opportunity_raw');

        INSERT INTO silver.learner_opportunity_raw (learner_id, opportunity_id, assigned_cohort, apply_date, status)
        SELECT 
            SUBSTRING(enrollment_id, 9, LEN(enrollment_id)) AS learner_id,
            SUBSTRING(learner_id, 13, LEN(learner_id)) AS opportunity_id,
            assigned_cohort,
            CAST(REPLACE(REPLACE(apply_date, 'Z', ''), 'T', ' ') AS DATETIME2) AS apply_date,
            status
        FROM bronze.learner_opportunity_raw
        WHERE apply_date != 'NULL';

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.learner_opportunity_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.learner_raw
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.learner_raw');

        INSERT INTO silver.learner_raw (learner_id, country, degree, institution, major)
        SELECT 
            SUBSTRING(learner_id, 9, LEN(learner_id)) AS learner_id,
            CASE 
                WHEN country != 'NULL' THEN REPLACE(REPLACE(LOWER(country), '"', ''), 'd%27', '''')
                ELSE 'n/a'
            END AS country,
            CASE 
                WHEN degree = 'Parent of Student' THEN 'not in education'
                WHEN degree IS NULL THEN 'n/a'
                WHEN degree LIKE '%Student' THEN LOWER(TRIM(REPLACE(degree, 'Student', '')))
                ELSE LOWER(degree)
            END AS degree,
            CASE 
                WHEN LOWER(institution) LIKE '%louis%' THEN 'saint louis university'
                WHEN LEN(institution) <= 2 OR institution IN ('10','100') OR institution IS NULL THEN 'n/a'
                ELSE LOWER(TRIM(institution))
            END AS institution,
            CASE 
                WHEN major IS NULL THEN 'n/a'
                ELSE LOWER(TRIM(major))
            END AS major
        FROM bronze.learner_raw;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.learner_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.opportunity_raw
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.opportunity_raw');

        INSERT INTO silver.opportunity_raw (opportunity_id, opportunity_name, category, opportunity_code)
        SELECT 
            SUBSTRING(opportunity_id, 13, LEN(opportunity_id)) AS opportunity_id,
            opportunity_name,
            category,
            opportunity_code
        FROM bronze.opportunity_raw;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.opportunity_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.tracking_questions
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.tracking_questions');

        INSERT INTO silver.tracking_questions (opportunity_id, code, is_frozen, is_required_for_badge_award)
        SELECT
            SUBSTRING(opportunity_id, 13, LEN(opportunity_id)) AS opportunity_id,
            code,
            is_frozen,
            is_required_for_badge_award
        FROM bronze.tracking_questions;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.tracking_questions in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -------------------------------------------------------------------
        -- silver.marketing_23_24
        -------------------------------------------------------------------
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: silver.marketing_23_24');

        INSERT INTO silver.marketing_23_24 
            (ad_account_name, campaign_name, delivery_status, delivery_level, reach, outbound_click, outbound_type, result_type, results, cost_per_result, amount_spend_aed, cpc)
        SELECT 
            ad_account_name,
            campaign_name,
            delivery_status,
            delivery_level,
            reach,
            ISNULL(outbound_click, 0),
            ISNULL(outbount_type, 0),
            result_type,
            results,
            cost_per_result,
            amount_spend_aed,
            ISNULL(cpc, 0)
        FROM bronze.marketing_23_24;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded silver.marketing_23_24 in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

    END TRY

    BEGIN CATCH
        PRINT('===================================================');
        PRINT('Error Occurred During Silver Load');
        PRINT('Error Message:' + ERROR_MESSAGE());
        PRINT('Error Number:' + ERROR_NUMBER());
        PRINT('Error State:' + ERROR_STATE());
        PRINT('===================================================');
    END CATCH;

    SET @end_time = GETDATE();
    PRINT('---------------------------------------------------');
    PRINT('Silver Load Completed');
    PRINT('End Time: ' + CONVERT(VARCHAR(30), @end_time, 120));
    PRINT('Total Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds');
    PRINT('---------------------------------------------------');

END;
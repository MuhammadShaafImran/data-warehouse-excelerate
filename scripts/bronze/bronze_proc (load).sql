CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE 
        @start_time DATETIME = GETDATE(),
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME

    PRINT('---------------------------------------------------');
    PRINT('Starting Bronze Load Process');
    PRINT('Start Time: ' + CONVERT(VARCHAR(30), @start_time, 120));
    PRINT('---------------------------------------------------');

    BEGIN TRY
        -- =============================
        -- Cognito_raw2
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.cognito_raw2 at ' + CONVERT(VARCHAR(30), @batch_start_time, 120));

        TRUNCATE TABLE bronze.cognito_raw2;

        BULK INSERT bronze.cognito_raw2
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\Cognito_Raw2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.cognito_raw2 in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- CohortRaw
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.cohortraw');

        TRUNCATE TABLE bronze.cohortraw;

        BULK INSERT bronze.cohortraw
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\CohortRaw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.cohortraw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- Learner Opportunity Raw
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.learner_opportunity_raw');

        TRUNCATE TABLE bronze.learner_opportunity_raw;

        BULK INSERT bronze.learner_opportunity_raw
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\LearnerOpportunity_Raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.learner_opportunity_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- Learner Raw
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.learner_raw');

        TRUNCATE TABLE bronze.learner_raw;

        BULK INSERT bronze.learner_raw
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\Learner_raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.learner_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- Marketing 23_24
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.marketing_23_24');

        TRUNCATE TABLE bronze.marketing_23_24;

        BULK INSERT bronze.marketing_23_24
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\Marketing Campaign Data All Accounts (2023-2024).csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.marketing_23_24 in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- Opportunity Raw
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Loading Table: bronze.opportunity_raw');

        TRUNCATE TABLE bronze.opportunity_raw;

        BULK INSERT bronze.opportunity_raw
        FROM 'D:\Dell_precision_3551\Data Analysis\Excelerate Internship\Data\Opportunity_Raw.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Loaded bronze.opportunity_raw in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

        -- =============================
        -- Tracking Questions
        -- =============================
        SET @batch_start_time = GETDATE();
        PRINT('>> [START] Creating Table: bronze.tracking_questions');

        IF OBJECT_ID('bronze.tracking_questions', 'U') IS NOT NULL
            DROP TABLE bronze.tracking_questions;

        SELECT
            t.opportunity_id,
            j.code,
            j.question,
            j.is_frozen,
            j.ans_type,
            j.is_required_for_badge_award
        INTO bronze.tracking_questions
        FROM bronze.opportunity_raw t
        CROSS APPLY OPENJSON(
            REPLACE(
                SUBSTRING(
                    CAST(t.tracking_question AS NVARCHAR(MAX)), 
                    2, 
                    LEN(CAST(t.tracking_question AS NVARCHAR(MAX))) - 2
                ),
                '""',
                '"'
            )
        )
        WITH (
            code NVARCHAR(100),
            question NVARCHAR(MAX),
            is_frozen NVARCHAR(10),
            ans_type NVARCHAR(50),
            is_required_for_badge_award NVARCHAR(10)
        ) AS j
        WHERE t.tracking_question IS NOT NULL;

        SET @batch_end_time = GETDATE();
        PRINT('>> [SUCCESS] Created bronze.tracking_questions in ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR) + ' seconds');

    END TRY

    BEGIN CATCH
        
        PRINT('===================================================');
		PRINT('Error Occured During Loading Bronze Data');
		PRINT('Error Message:' + ERROR_MESSAGE());
		PRINT('Error Number:' + ERROR_NUMBER());
		PRINT('Error State:' + ERROR_STATE());
		PRINT('===================================================');

    END CATCH;

    SET @end_time = GETDATE();
    PRINT('---------------------------------------------------');
    PRINT('Bronze Load Completed Successfully');
    PRINT('End Time: ' + CONVERT(VARCHAR(30), @end_time, 120));
    PRINT('Total Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds');
    PRINT('---------------------------------------------------');
END;
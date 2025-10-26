USE DataWarehouse_Excelerate

-- Script to create tables (DDL) Silver Layer

-- Creating cognito_raw2 table in silver layer
if object_id('silver.cognito_raw2','U') is not null
 drop table silver.cognito_raw2;
create table silver.cognito_raw2 (
	userid nvarchar(50),
	email nvarchar(100),
	gender nvarchar(50),
	create_date datetime,
	last_modified_date datetime,
	age nvarchar(50),
	city nvarchar(50),
	zip nvarchar(50),
	state nvarchar(50)
);

go

-- Creating cohortraw table in silver layer
if object_id('silver.cohortraw','U') is not null 
	drop table silver.cohortraw;
create table silver.cohortraw(
	cohort_code nvarchar(50),
	start_date datetime2,
	end_date datetime2,
	size int
);

go

-- Creating learner_raw(in) table in silver layer
if object_id('silver.learner_raw','U') is not null 
	drop table silver.learner_raw;
create table silver.learner_raw(
	learner_id nvarchar(100),
	country nvarchar(50),
	degree nvarchar(50),
	institution nvarchar(150),
	major nvarchar(150)
);

go

-- Creating learner Opportunity Raw table in silver layer
if object_id('silver.learner_opportunity_raw','U') is not null
	drop table silver.learner_opportunity_raw;
create table silver.learner_opportunity_raw(
	learner_id nvarchar(150),
	opportunity_id nvarchar(150),
	assigned_cohort nvarchar(50),
	apply_date datetime2,
	status nvarchar(10),
);

go

-- Creating opportunity raw table in silver layer
if object_id('silver.opportunity_raw','U') is not null
	drop table silver.opportunity_raw;
create table silver.opportunity_raw(
	opportunity_id nvarchar(150),
	opportunity_name nvarchar(200),
	category nvarchar(50),
	opportunity_code nvarchar(50),
);

go

-- Creating marketing table in silver layer
if object_id('silver.marketing_23_24','U') is not null
	drop table silver.marketing_23_24;
create table silver.marketing_23_24(
	ad_account_name nvarchar(50),
	campaign_name text,
	delivery_status nvarchar(50),
	delivery_level nvarchar(50),
	reach int,
	outbound_click int,
	outbound_type int,
	result_type nvarchar(50),
	results int,
	cost_per_result float,
	amount_spend_aed float,
	cpc float,
);

go

if object_id('silver.tracking_questions','U') is not null
	drop table silver.tracking_questions;
create table silver.tracking_questions (
	opportunity_id nvarchar(100),
	code nvarchar(100),
    question nvarchar(MAX),
    is_frozen nvarchar(10),
    is_required_for_badge_award nvarchar(10)
)
-- Script to create tables (DDL)

-- Creating cognito_raw2 table in bronze layer
if object_id('bronze.cognito_raw2','U') is not null
 drop table bronze.cognito_raw2;
create table bronze.cognito_raw2 (
	userid nvarchar(50),
	email nvarchar(100),
	gender nvarchar(50),
	usercreatedate datetime,
	userlastmodifieddate datetime,
	birthdate nvarchar(50),
	city nvarchar(50),
	zip nvarchar(50),
	state nvarchar(50)
);

go

-- Creating cohortraw table in bronze layer
if object_id('bronze.cohortraw','U') is not null 
	drop table bronze.cohortraw;
create table bronze.cohortraw(
	cohort_id nvarchar(10),
	cohort_code nvarchar(50),
	start_date bigint,
	end_date bigint,
	size int
);

go 

-- Creating learner_raw(in) table in bronze layer
if object_id('bronze.learner_raw','U') is not null 
	drop table bronze.learner_raw;
create table bronze.learner_raw(
	learner_id nvarchar(100),
	country nvarchar(50),
	degree nvarchar(50),
	institution nvarchar(150),
	major nvarchar(150)
);

go

-- Creating learner Opportunity Raw table in bronze layer
if object_id('bronze.learner_opportunity_raw','U') is not null
	drop table bronze.learner_opportunity_raw;
create table bronze.learner_opportunity_raw(
	enrollment_id nvarchar(150),
	learner_id nvarchar(150),
	assigned_cohort nvarchar(50),
	apply_date nvarchar(100),
	status nvarchar(50),
);

go 

-- Creating opportunity raw table in bronze layer
if object_id('bronze.opportunity_raw','U') is not null
	drop table bronze.opportunity_raw;
create table bronze.opportunity_raw(
	opportunity_id nvarchar(150),
	opportunity_name nvarchar(200),
	category nvarchar(50),
	opportunity_code nvarchar(50),
	tracking_question text
);

go

-- Creating marketing table in bronze layer
if object_id('bronze.marketing_23_24','U') is not null
	drop table bronze.marketing_23_24;
create table bronze.marketing_23_24(
	ad_account_name nvarchar(50),
	campaign_name text,
	delivery_status nvarchar(50),
	delivery_level nvarchar(50),
	reach int,
	outbound_click int,
	outbount_type int,
	result_type nvarchar(50),
	results int,
	cost_per_result float,
	amount_spend_aed float,
	cpc float,
	reporting_stats nvarchar(10)
);
create or replace task CREATE_OOS_DEEPDIVE_TABLE
	warehouse=BI_P_QRY_WHS_WH
	schedule='USING CRON 0 9 * * * America/New_York'
	allow_overlapping_execution=true
	as create or replace table DB_BI_P_EDW.CCBCC_SELFSERVICE.PSP_DEEPDIVE_LIVE
as
select * from DB_BI_P_EDW.CCBCC_SELFSERVICE.PSP_DEEPDIVE_2023;
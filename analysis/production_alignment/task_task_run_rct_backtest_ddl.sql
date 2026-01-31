create or replace task TASK_RUN_RCT_BACKTEST
	warehouse=BI_P_QRY_LG_WH
	schedule='USING CRON 0 5 * * * America/New_York'
	COMMENT='Daily 5am: Run backtest/forecast for RCT'
	as CALL SANDBOX.RUN_RCT_BACKTEST(6, 3, 5);
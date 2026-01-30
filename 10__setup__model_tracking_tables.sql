-- 10__setup__model_tracking_tables.sql

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_EXPERIMENTS (
  experiment_id        string,
  experiment_name      string,
  experiment_desc      string,
  created_by           string,
  created_at           timestamp_ntz,
  tags                 variant,
  primary key (experiment_id)
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_RUNS (
  model_run_id         string,
  run_id               string,               -- ties to FORECAST_RUNS.run_id
  asof_fiscal_yyyymm    number,

  experiment_id        string,
  model_scope          string,               -- GLOBAL | PER_PC_REASON (and later: PER_PC, etc.)
  model_family         string,               -- e.g., ridge, lgbm, xgb, prophet, etc.
  feature_set_id       string,               -- logical name/version (e.g. "fs_v1")
  target_name          string,               -- "TOTAL_REVENUE"

  train_anchor_min_seq number,
  train_anchor_max_seq number,
  max_horizon          number,               -- typically 12

  params               variant,              -- hyperparams / model config
  training_env         variant,              -- {runner: "snowflake"|"local", python, package versions, etc.}
  code_ref             variant,              -- {notebook: "...", git_commit: "...", script: "..."} (optional)

  status               string,               -- STARTED | SUCCEEDED | FAILED
  status_message       string,
  started_at           timestamp_ntz,
  ended_at             timestamp_ntz,
  updated_at           timestamp_ntz,

  primary key (model_run_id)
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_METRICS (
  model_run_id         string,
  metric_scope         string,               -- OVERALL | BY_HORIZON
  metric_name          string,               -- WAPE | SMAPE | RMSE | MAE (etc.)
  horizon              number,               -- null for OVERALL
  value                float,

  computed_at          timestamp_ntz,
  details              variant               -- optional: denominators, counts, filters
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_BACKTEST_PREDICTIONS (
  model_run_id         string,

  roll_up_shop         string,
  reason_group         string,

  anchor_fiscal_yyyymm number,
  anchor_month_seq     number,
  horizon              number,

  target_fiscal_yyyymm number,
  target_month_seq     number,

  y_true               number(18,2),
  y_pred               number(18,2),

  y_pred_lo            number(18,2),
  y_pred_hi            number(18,2),

  created_at           timestamp_ntz,
  details              variant               -- optional: features used, residuals, etc.
);

create or replace table DB_BI_P_SANDBOX.SANDBOX.FORECAST_MODEL_CHAMPIONS (
  asof_fiscal_yyyymm    number,

  champion_scope        string,              -- GLOBAL | PC_REASON
  roll_up_shop          string,              -- null when GLOBAL
  reason_group          string,              -- null when GLOBAL

  model_run_id          string,
  selection_metric      string,              -- e.g., "WAPE_OVERALL"
  selection_logic       variant,             -- thresholds, tie-breaks, etc.

  selected_at           timestamp_ntz,
  selected_by           string,

  primary key (asof_fiscal_yyyymm, champion_scope, roll_up_shop, reason_group)
);

show tables like 'FORECAST_MODEL_%' in schema DB_BI_P_SANDBOX.SANDBOX;

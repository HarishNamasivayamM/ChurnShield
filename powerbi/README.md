# Power BI handoff

The local pipeline writes `artifacts/predictions.csv`, which can be imported
directly into Power BI Desktop. The prediction output contains one row per
customer and includes:

- `CHURN_PROBABILITY`, `CHURN_FLAG`, and `RISK_LEVEL`
- `RISK_FACTORS` as compact JSON for drill-through or detail views
- customer tenure, monthly charges, total charges, and contract
- `ACTUAL_CHURN_FLAG` when the historical `Churn` label is available

For a warehouse-backed report, run `sql/001_snowflake_setup.sql`, load the
scored rows into `CHURN_ANALYSIS.PREDICTIONS.CHURN_PREDICTIONS`, and connect
Power BI to that table or a view built on it. Configure credentials and the
data source in Power BI Desktop; they are intentionally not stored here.

The supplied PBIX is a reference snapshot whose model table is named
`churn_dataset`. It is not automatically rebound to the generated prediction
table, so refresh its source or create a matching view before publishing.

# Generated artifacts

This directory is intentionally kept in the repository so Docker Compose has
a host-side mount point for generated files. Runtime outputs are ignored by
Git and may include:

- `model.joblib` - trained preprocessing and classifier bundle
- `metrics.json` - holdout metrics and grouped feature importance
- `predictions.csv` - batch predictions for Power BI or other BI tools
- `stream_predictions.csv` - predictions appended by the Kafka consumer

Regenerate the files with `python run_pipeline.py` from the repository root.

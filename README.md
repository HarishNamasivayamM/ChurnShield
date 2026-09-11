# ChurnStream

ChurnStream is an end-to-end customer-churn pipeline built around the supplied
Telco Customer Churn dataset. It supports a reproducible local batch workflow,
Kafka event publishing and scoring, and optional Snowflake/Power BI handoff.

```text
churn_dataset.csv
       |
       +--> train_model.py --> artifacts/model.joblib + metrics.json
       |                                  |
       +--> run_pipeline.py --> predictions.csv
       |
       +--> Kafka customer_events --> kafka_consumer_churn.py
                                             |
                                             +--> stream_predictions.csv
```

The local path does not require Kafka, Snowflake, or Power BI. External
services are explicit adapters and require the user's own credentials and
connector installation.

## Implementation status

Implemented and tested in this repository:

- CSV validation, type normalization, and JSON-safe Kafka publishing
- Reproducible Random Forest training with preprocessing and holdout metrics
- Saved model bundle, batch predictions, risk levels, and ranked rule-based
  risk factors
- Kafka consumer that scores incoming customer events and appends predictions
- Single-broker Kafka and Kafka Connect Docker Compose configuration
- Snowflake DDL and a connector configuration template with no credentials
- Power BI handoff instructions and GitHub Actions CI

Not bundled:

- A Snowflake sink connector binary, Snowflake account, or credentials
- SHAP explanations; `RISK_FACTORS` are transparent business rules ranked by
  grouped model feature importance
- An automatically refreshed or published Power BI report

The supplied Power BI, PDF, and Word files are reference/report artifacts. The
source-code license does not automatically grant redistribution rights for
those artifacts or the bundled dataset; see
[`DATA-AND-REPORT-NOTICE.md`](DATA-AND-REPORT-NOTICE.md).

## Requirements

- Python 3.10 or newer
- Docker Desktop with Docker Compose v2 for the Kafka path
- A Snowflake account only if the optional warehouse step is needed

## Local batch quick start

Create a virtual environment and install the runtime dependencies:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Train, evaluate, and score all supplied customer rows:

```bash
python run_pipeline.py --input churn_dataset.csv --output-dir artifacts
```

The command writes:

- `artifacts/model.joblib` - preprocessing and classifier bundle
- `artifacts/metrics.json` - accuracy, precision, recall, F1, ROC AUC, and
  grouped feature importance
- `artifacts/predictions.csv` - BI-friendly scored customer records

For separate training and scoring entry points:

```bash
python train_model.py --input churn_dataset.csv --output-dir artifacts
```

The installed package also exposes `churn-pipeline`, `churn-train`,
`churn-consumer`, and `churn-producer` commands after `python -m pip install -e .`.

## Kafka streaming path

Start Kafka and Kafka Connect:

```bash
docker compose up -d
docker compose ps
```

Train a model locally, then publish a small smoke-test batch:

```bash
python train_model.py --input churn_dataset.csv --output-dir artifacts
python kafka_producer_churn.py --limit 100 --rate 20 --topic customer_events
```

In another terminal, consume and score those events:

```bash
python kafka_consumer_churn.py \
  --bootstrap-servers localhost:9092 \
  --topic customer_events \
  --model artifacts/model.joblib \
  --output artifacts/stream_predictions.csv \
  --from-beginning \
  --group-id churnstream-local \
  --max-messages 100
```

The consumer appends scored rows to `artifacts/stream_predictions.csv`.
Use a new `--group-id` with `--from-beginning` when replaying a topic. Host
clients use `localhost:9092`; containers use `kafka:29092`.

The same path can be run with the application image. The `pipeline` Compose
profile provides a trainer and a container-network Kafka consumer:

```bash
docker compose --profile pipeline build
docker compose --profile pipeline run --rm trainer
python kafka_producer_churn.py --limit 100 --rate 20 --topic customer_events
docker compose --profile pipeline run --rm consumer
```

Generated files are written to the host `artifacts/` directory and are
ignored by Git.

## Producer options

```text
--csv PATH                 Input CSV path
--bootstrap-servers VALUE  Comma-separated Kafka brokers
--topic NAME               Kafka topic
--rate NUMBER              Maximum rows per second; 0 means no rate limit
--limit NUMBER             Maximum rows per pass
--loop                     Replay the input continuously
--dry-run                  Validate and preview without connecting to Kafka
```

The same settings can be provided with `KAFKA_BOOTSTRAP_SERVERS` and
`KAFKA_TOPIC`. `.env.example` documents supported environment values; the
producer does not load a `.env` file automatically.

The producer strips whitespace, converts `SeniorCitizen` and `tenure` to
integers, converts charge fields to numbers, and turns blank values into JSON
`null`. This handles the 11 whitespace-only `TotalCharges` cells in the
supplied dataset without emitting invalid JSON `NaN` values. Messages use
`customerID` as their Kafka key.

## Snowflake handoff

Run [`sql/001_snowflake_setup.sql`](sql/001_snowflake_setup.sql) in Snowflake
to create the `CHURN_ANALYSIS` database and its raw, feature, and prediction
tables/views. The prediction table matches the columns produced by the local
scoring path.

The Kafka Connect service is available at `http://localhost:8083`, but the
base image does not include the Snowflake sink connector. Obtain a compatible
connector distribution from Snowflake, place its files under
`connect-plugins/`, and restart Connect. Then adapt
[`config/snowflake-connector.example.json`](config/snowflake-connector.example.json)
and submit it to the Connect REST API:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  --data @config/snowflake-connector.example.json
```

The example uses a private-key placeholder. Keep private keys, passwords, and
account identifiers outside GitHub and use the connector's supported secret
management approach. For optional Python-side Snowflake work, install
`requirements-snowflake.txt`; it is not needed for local training or scoring.

To load local scored predictions into the Snowflake prediction table, install
the optional requirements and set `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, and
`SNOWFLAKE_PASSWORD` (plus warehouse/role variables when required):

```bash
python -m pip install -r requirements-snowflake.txt
python snowflake_loader.py --input artifacts/predictions.csv
```

Validate the file and SQL target without connecting by adding `--dry-run`.
The loader uses batched inserts and parses `RISK_FACTORS` into Snowflake
`VARIANT` values.

## Power BI

Import `artifacts/predictions.csv` into Power BI Desktop for a local report,
or connect Power BI to `CHURN_ANALYSIS.PREDICTIONS.CHURN_PREDICTIONS` after
loading scored rows into Snowflake. See
[`powerbi/README.md`](powerbi/README.md) for the output contract and the
important PBIX rebinding note.

The supplied `Churn Analysis project.pbix` is a reference snapshot whose model
table is named `churn_dataset`; it is not automatically rebound to generated
predictions. `Project_steps.docx` is an earlier walkthrough and mentions the
old topic name `my_kafka_topic`; the current topic is `customer_events`.

## Tests and CI

The test suite does not require Kafka:

```bash
python -m unittest discover -s tests -v
```

Run the same checks locally as CI:

```bash
python -m pip install -r requirements-dev.txt
ruff check .
ruff format --check .
```

GitHub Actions tests Python 3.10 through 3.13, validates the dataset, runs the
batch pipeline, and checks the Compose configuration. Docker is not required
for the local unit tests, but is required for the Kafka services.

## Data source

`churn_dataset.csv` contains 7,043 customer records and 21 columns from the
standard Telco Customer Churn dataset. `Churn` is the historical label used
for offline training; the producer includes it in each event for downstream
loading or replayable experiments.

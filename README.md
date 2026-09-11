# ChurnStream

ChurnStream publishes the supplied Telco customer dataset to Kafka as JSON
events. The repository also contains Snowflake SQL and Power BI/Word/PDF
reference artifacts for the broader churn-analysis workflow.

The runnable local path is:

```text
churn_dataset.csv -> kafka_producer_churn.py -> Kafka topic customer_events
```

Snowflake ingestion, model training, and Power BI refresh require external
services and credentials. The source tree does not contain a Snowflake
connector configuration with credentials or an ML training script, so those
steps are intentionally optional rather than implied by the local quick start.

## Implementation status

Implemented and tested in this repository:

- CSV validation and JSON normalization
- Host-to-container Kafka publishing
- Single-broker Kafka and Kafka Connect Compose configuration
- Snowflake table/view definitions
- Offline producer tests and GitHub Actions CI

Not included in this repository:

- A Snowflake sink connector binary or credentials
- Model training, inference, or SHAP scoring code
- A live Power BI connection or refresh service

The supplied Power BI, PDF, and Word files are reference/report artifacts.
The source-code license does not automatically grant redistribution rights for
those artifacts or the bundled dataset; see
[`DATA-AND-REPORT-NOTICE.md`](DATA-AND-REPORT-NOTICE.md).

## Requirements

- Python 3.10 or newer
- Docker Desktop with Docker Compose v2 for local Kafka
- A Snowflake account only if the optional warehouse step is needed

## Local quick start

Create a virtual environment and install the only runtime dependency:

```bash
python -m venv .venv
source .venv/bin/activate          # Windows PowerShell: .venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Start the single-broker Kafka and Kafka Connect services:

```bash
docker compose up -d
docker compose ps
```

Validate the dataset without needing Kafka:

```bash
python kafka_producer_churn.py --dry-run --limit 3
```

Publish all 7,043 rows to the default topic:

```bash
python kafka_producer_churn.py --rate 20
```

For a small smoke test, publish only ten rows:

```bash
python kafka_producer_churn.py --limit 10 --topic customer_events
```

Consume a few messages from another terminal:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:29092 \
  --topic customer_events \
  --from-beginning \
  --max-messages 3
```

Host-side clients use `localhost:9092`; containers use `kafka:29092`. This
two-listener setup is required so both the Python producer and Kafka Connect
can reach the same broker.

Stop the services when finished:

```bash
docker compose down
```

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
`KAFKA_TOPIC`. `.env.example` documents the supported environment values;
the producer does not load a `.env` file automatically.

The producer strips whitespace, converts `SeniorCitizen` and `tenure` to
integers, converts charge fields to numbers, and turns blank values into JSON
`null`. This handles the 11 whitespace-only `TotalCharges` cells in the
supplied dataset without emitting invalid JSON `NaN` values. Messages use
`customerID` as their Kafka key.

## Snowflake setup

Run [`sql/001_snowflake_setup.sql`](sql/001_snowflake_setup.sql) in Snowflake
to create the `CHURN_ANALYSIS` database, `RAW_DATA`, `FEATURES`, and
`PREDICTIONS` schemas, plus example landing, feature, and prediction tables.

The Kafka Connect service is ready on `http://localhost:8083`, but the base
image does not include the Snowflake sink connector. Install a compatible
Snowflake connector distribution into `connect-plugins/` and create the sink
connector through the Connect REST API. Keep connector binaries and all
credentials out of source control.

## Tests

The test suite does not require Kafka:

```bash
python -m unittest discover -s tests -v
```

The project also supports an editable install, which exposes a
`churn-producer` command:

```bash
python -m pip install -e .
churn-producer --dry-run --limit 3
```

## Supplied report artifacts

- `Churn Analysis project.pbix` is a Power BI report package.
- `PowerBiReport.pdf` is an exported report snapshot.
- `Churn_Analysis_Report.docx` and `Project_steps.docx` are narrative/reference documents.

The PBIX package is structurally valid and its layout references a model
table named `churn_dataset`. Its package metadata contains Power BI remote
artifact IDs rather than a visible Snowflake connection, so configure the
data source in Power BI Desktop when moving the report to a Snowflake-backed
model. `Project_steps.docx` is an earlier walkthrough and mentions the old
topic name `my_kafka_topic`; use `customer_events` from this README and the
producer as the current topic. These files are not required to run the local
Kafka producer.

## Data source

`churn_dataset.csv` contains 7,043 customer records and 21 columns from the
standard Telco Customer Churn dataset. `Churn` is the historical label; the
producer sends it as part of each event for downstream loading or offline
training.

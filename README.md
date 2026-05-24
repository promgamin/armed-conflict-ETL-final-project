# Armed Conflict ETL Final Project

ETL pipeline for analyzing victims of the armed conflict in Colombia. Integrates two data sources, validates data quality, builds a dimensional model, streams metrics via Kafka, and visualizes results through static and real-time dashboards.


## Overview
It processes historical records of armed conflict victims in Colombia from 2012 to 2026, combining a local SQLite database with a national public API.

The pipeline is orchestrated by Apache Airflow and supports two analytical modes:

- **Batch (analytical):** builds a star schema in MySQL for historical analysis via Metabase.
- **Streaming (operational):** streams fact table records through Kafka and visualizes them in real time with Streamlit.

---

## Tech Stack

| Layer | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8.1 |
| Data Warehouse | MySQL 8.0 |
| Data Quality | Great Expectations 0.17.23 |
| Streaming | Apache Kafka + Zookeeper (Confluent 7.5.0) |
| Static Dashboard | Metabase |
| Real-time Dashboard | Streamlit + Plotly |
| Language | Python 3.11 |
| Containerization | Docker + Docker Compose |

---

## Project Structure

```
armed-conflict-ETL-final-project/
├── dag/
│   └── dag.py                      # Airflow DAG definition
├── data/
│   ├── raw/                        # Raw source files (excluded from git)
│   └── processed/                  # Intermediate parquets and dimensions
├── notebooks/
│   ├── eda.ipynb
│   ├── extraction_transformation.ipynb
│   └── load_visualization.ipynb
├── scripts/
│   ├── ingest_source1.py           # Reads SQLite database
│   ├── ingest_source2.py           # Consumes public API with pagination
│   ├── transform_source1.py        # Cleans and normalizes source 1
│   ├── transform_source2.py        # Cleans and normalizes source 2
│   ├── concat_sources.py           # Merges both sources
│   ├── validate.py                 # Great Expectations validation
│   ├── dimensions.py               # Builds dimension and fact parquets
│   └── load.py                     # Inserts parquets into MySQL
├── kafka_streaming/
│   ├── producer.py                 # Reads fact table and sends to Kafka
│   ├── consumer.py                 # Kafka consumer (used by Streamlit)
│   └── visualization_streamlit.py  # Real-time dashboard
├── great_expectations/
│   └── expectations/
│       └── suite_dataset_final.json
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── .gitignore
```


## Data Sources

| Source | Type | Description | Records |
|---|---|---|---|
| Source 1 | SQLite DB | Victims from Cali (Open Data) | Local |
| Source 2 | REST API | National victims — datos.gov.co (ynab-fjc9) | ~1,854,370 |

After extraction, transformation, and aggregation both sources are merged into a single dataset of 103,507 rows.


## Pipeline

```
ingest_source1 → transform_source1 ──┐
                                      ├→ concat → validate → build_dimensions → load → kafka_stream
ingest_source2 → transform_source2 ──┘
```

| Task | Description |
|---|---|
| `ingest_source1_cali` | Reads SQLite and saves to parquet |
| `ingest_source2_api` | Paginates API and saves to parquet |
| `transform_source1_cali` | Normalizes text, departments, ethnicity, age ranges |
| `transform_source2_api` | Same as above for national data |
| `concat_sources` | Merges both transformed parquets |
| `validate_great_expectations` | Runs 50+ data quality checks |
| `build_dimensions` | Builds star schema parquets |
| `load_mysql` | Truncates and reloads all tables in MySQL |
| `kafka_streaming` | Streams fact table rows to Kafka topic |

Schedule: `0 6 * * *` (daily at 6 AM)

---

## Dimensional Model

Star schema stored in `dw_armed_conflict` MySQL database.

**Dimension tables:**

| Table | Columns |
|---|---|
| `person` | id_person, sex, ethnic_group, age_range |
| `victimizing_act` | id_act, victimization_fact |
| `location` | id_location, state_dept |
| `registration_date` | date_processing, year, month |

**Fact table:**

| Table | Columns |
|---|---|
| `victims` | id_person, id_act, id_location, date_processing |

---

## Dashboards

### Static Dashboard — Metabase (port 3000)

Connected to `mysql_dw`. Displays:

- Top 5 departments by ethnic groups most affected
- Age groups with highest number of records
- Top 5 victimizing facts: Santander vs Valle del Cauca
- Months with highest number of reports
- Sex distribution of affected population
- Evolution of forced displacement (2012–2026)

### Real-time Dashboard — Streamlit (port 8501)

Consumes Kafka topic `armed_conflict_metrics`. Displays:

- Cumulative record counter
- Top 5 departments (live horizontal bar chart)
- Displacement vs other facts proportion (live line chart)
- Last 5 records received (live table)

---

## Setup

### Requirements

- Docker
- Docker Compose

### Steps

1. Clone the repository:

```bash
git clone https://github.com/<your-username>/armed-conflict-ETL-final-project.git
cd armed-conflict-ETL-final-project
```

2. Start all services:

```bash
docker-compose up --build -d
```

3. Wait for all services to be healthy, then access:

| Service | URL | Credentials |
|---|---|---|
| Airflow | http://localhost:8080 | admin / admin |
| Metabase | http://localhost:3000 
| Streamlit | http://localhost:8501 | — |
| MySQL DW | localhost:3308 | etl_user / etl_password |

4. In Airflow, run the DAG `dag_armed_conflict_victims`.

5. Open Streamlit to see the real-time stream.

---

## Usage

### Run the DAG manually

```bash
docker exec -it airflow_scheduler airflow dags trigger dag_armed_conflict_victims
```

### Access MySQL DW

```bash
docker exec -it mysql_dw mysql -u etl_user -petl_password dw_armed_conflict
```

### Stop all services

```bash
docker-compose down
```
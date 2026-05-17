from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.ingest_source1 import ingest_source1
from scripts.ingest_source2 import ingest_source2
from scripts.transform_source1 import transform_source1
from scripts.transform_source2 import transform_source2
from scripts.concat_sources import run_concat
from scripts.validate import validate_all
from scripts.dimensions import build_dimensions
from scripts.load import load_to_mysql

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="dag_armed_conflict_victims",
    default_args=default_args,
    description="ETL pipeline for armed conflict victims data in Colombia",
    schedule_interval="0 6 * * *",
    start_date=datetime(2026, 4, 1),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "victims", "sdg16"],
) as dag:

# Ingest 

    task_ingest_f1 = PythonOperator(
        task_id="ingest_source1_cali",
        python_callable=ingest_source1,
        op_kwargs={
            "sqlite_path": "/opt/airflow/data/processed/Project_ETL.db",
            "output_path": "/opt/airflow/data/processed/source1.parquet",
        },
    )

    task_ingest_f2 = PythonOperator(
        task_id="ingest_source2_api",
        python_callable=ingest_source2,
        op_kwargs={
            "url": "https://www.datos.gov.co/resource/ynab-fjc9.json",
            "start_year": 2012,
            "output_path": "/opt/airflow/data/processed/source2.parquet",
        },
    )


# Transformation 
    task_transform_f1 = PythonOperator(
        task_id="transform_source1_cali",
        python_callable=transform_source1,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source1.parquet",
            "output_path": "/opt/airflow/data/processed/source1_transformed.parquet",
        },
    )

    task_transform_f2 = PythonOperator(
        task_id="transform_source2_api",
        python_callable=transform_source2,
        op_kwargs={
            "input_path": "/opt/airflow/data/processed/source2.parquet",
            "output_path": "/opt/airflow/data/processed/source2_transformed.parquet",
        },
    )

# Concatenation 
    task_concat = PythonOperator(
        task_id="concat_sources",
        python_callable=run_concat,
        op_kwargs={
            "source1_path": "/opt/airflow/data/processed/source1_transformed.parquet",
            "source2_path": "/opt/airflow/data/processed/source2_transformed.parquet",
            "output_path": "/opt/airflow/data/processed/dataset_final.parquet",
        },
    )

# Validation
    task_validate=PythonOperator(
        task_id="validate_great_expectations",
        python_callable=validate_all,
        op_kwargs={
            "dataset_final_path": "/opt/airflow/data/processed/dataset_final.parquet",
            "gx_root": "/opt/airflow/great_expectations",
        },
    )

# build dimensions
    task_dimensions=PythonOperator(
        task_id="build_dimensions",
        python_callable=build_dimensions,
        op_kwargs={
            "input_path" : "/opt/airflow/data/processed/dataset_final.parquet",
            "output_dir" : "/opt/airflow/data/processed",
        },
    )

# load
    task_load=PythonOperator(
        task_id="load_mysql",
        python_callable=load_to_mysql,
        op_kwargs={
            "processed_dir": "/opt/airflow/data/processed",
        },
    )
    

# Workflow 

    task_ingest_f1 >> task_transform_f1
    task_ingest_f2 >> task_transform_f2

    [task_transform_f1, task_transform_f2] >> task_concat
    task_concat >> task_validate >> task_dimensions >> task_load
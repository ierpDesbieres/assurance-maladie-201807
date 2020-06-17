import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from data_aggregations.aggregations import (
  aggregate,
  compute_average_by_speciality, 
  compute_repartition_by_speciality,
  compute_total_by_speciality, 
  csv_to_sql,
  load_clean_data
)
from data_aggregations.config import config
from data_aggregations.csv_sanity_check import df_sanity_check, csv_test_cases
from data_aggregations.sql_sanity_check import (
  sql_sanity_checks,
  avg_test_cases,
  repartition_test_cases
)


session = sessionmaker()(bind=create_engine(config.db_url))


default_args = {
    "owner": "pierre_dembiermont",
    "depends_on_past": False,
    "start_date": datetime(2020, 6, 16),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG("data_aggregations", default_args=default_args, catchup=False) as dag:
  csv_sanity_check = ShortCircuitOperator(
    task_id='csv_sanity_check',
    provide_context=False,
    python_callable=df_sanity_check,
    op_kwargs= {
        "test_cases": csv_test_cases(
          source_csv=config.csv_path,
          csv_args=config.csv_args
        ),
    }
  )


  aggregations_task = PythonOperator(
      task_id='aggregate_data',
      provide_context=False,
      python_callable=aggregate,
      op_kwargs={
        "csv": config.csv_path,
        "csv_args": config.csv_args,
        "to_csv": True
      }
  )


  avg_to_psql_task = PythonOperator(
      task_id='avg_to_psql',
      provide_context=False,
      python_callable=csv_to_sql,
      op_kwargs={
        "csv_path": os.path.join(config.data_dir, "avg_by_speciality.csv"),
        "request_path": os.path.join(config.requests_path, "aggregations/create_load_avg_schema.sql"),
        "session": session
      }
  )


  check_avg_to_psql_task = ShortCircuitOperator(
      task_id='check_avg_to_psql',
      provide_context=False,
      python_callable=sql_sanity_checks,
      op_kwargs={
        "test_cases": avg_test_cases,
        "session": session
      }
  )


  repartition_to_psql_task = PythonOperator(
      task_id='repartition_to_psql',
      provide_context=False,
      python_callable=csv_to_sql,
      op_kwargs={
        "csv_path": os.path.join(config.data_dir, "repartition_by_speciality.csv"),
        "request_path": os.path.join(config.requests_path, "aggregations/create_load_repartition_schema.sql"),
        "session": session
      }
  )


  check_repartition_to_psql_task = ShortCircuitOperator(
      task_id='check_repartition_to_psql',
      provide_context=False,
      python_callable=sql_sanity_checks,
      op_kwargs={
        "test_cases": repartition_test_cases,
        "session": session,
      }
  )


  csv_sanity_check >> aggregations_task >> [avg_to_psql_task, repartition_to_psql_task]
  avg_to_psql_task >> check_avg_to_psql_task
  repartition_to_psql_task >> check_repartition_to_psql_task
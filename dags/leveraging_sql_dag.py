import os

import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


from data_aggregations.alternate_aggregations import load_data, sql_aggregation
from data_aggregations.config import config
from data_aggregations.csv_sanity_check import df_sanity_check, csv_test_cases
from data_aggregations.sql_sanity_check import (
  reimbursements_test_cases, 
  avg_test_cases, 
  repartition_test_cases, 
  sql_sanity_checks
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
    'depends_on_past' : False
}


with DAG("alternate_data_aggregations", default_args=default_args, catchup=False) as dag:
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


  load_data_task = PythonOperator(
    task_id='load_data',
    provide_context=False,
    python_callable=load_data,
    op_kwargs={
      "csv": config.csv_path,
      "csv_args": config.csv_args,
      "session": session
    }
  )


  reimbursements_sanity_check = ShortCircuitOperator(
    task_id='reimbursement_sanity_check',
    dag=dag,
    python_callable=sql_sanity_checks,
    op_kwargs= {
        "test_cases": reimbursements_test_cases,
        "session": session
    }

  )


  avg_task = PythonOperator(
    task_id='compute_avg_by_speciality',
    provide_context=False,
    python_callable=sql_aggregation,
    op_kwargs={
      "request": os.path.join(
        config.requests_path, "alternate_aggregations/avg_amount_by_speciality.sql"
      ),
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


  repartition_task = PythonOperator(
    task_id='compute_repartition_by_speciality',
    provide_context=False,
    python_callable=sql_aggregation,
    op_kwargs={
      "request": os.path.join(
        config.requests_path, "alternate_aggregations/amount_repartition_by_speciality.sql"
      ),
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

  csv_sanity_check >> load_data_task >> reimbursements_sanity_check >> [avg_task, repartition_task]
  avg_task >> check_avg_to_psql_task
  repartition_task >> check_repartition_to_psql_task
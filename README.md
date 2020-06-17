# Dépenses de l'Assurance Maladie - Juillet 2018

![CI depenses assurance maladie 201807](https://github.com/ierpDesbieres/depenses-assurance-maladie-201807/workflows/CI%20depenses%20assurance%20maladie%20201807/badge.svg)

## Getting started
```zsh
docker-compose up
```
You can then navigate to the airflow UI at [localhost:8080](localhost:8080)

### Dependencies
```
apache-airflow==1.10.10
mysqlclient==1.4.6
pandas==1.0.0
psycopg2-binary==2.8.5
SQLAlchemy==1.3.17
typing-extensions==3.7.4.2
```

### Tests

I provide tests for the computations (average and repartition in `tests/test_aggregations.py`) as well as some basic testing of DAGs definition (in `tests/test_dags.py`).
To run them just do (it will take some time to run cause of packages installations and àirflow initdb`)
```zsh
docker-compose -f docker-compose.test.yml
```

## DAGS
There is two DAGs that solve the same problem but with two different methods. You can find the sql requests used by both methods in `/requests` and the code used in `/dags/data_aggregations`.

### `aggregations_dag.py`
This dag leverage mostly python for everything, including the average and the amount repartition calculations.
This one is pretty much straightforward:
  - `load_clean_data` load csv as pandas.DataFrame, convert `rem_mon` to float, rename some columns and select columns of interest. We skip rows with `rem_mon` = 0 as they are just an absence of reimbursement in this category for the month (will impact the average if not). We could also choose to ignore negative rows are they are regularizations of previous months and should ,maybe, not be accounted in the current month (the option is in `load_data` with the parameter `skip_negatives`) keeping them depends of the problem we want to solve.
  - `compute_total_by_speciality` compute the total amount by speciality
  - `compute_average_by_speciality` compute the average by speciality
  - `compute_repartition_by_speciality` compute the repartition of the amount (% of total amount) by speciality
  - `aggregate` group all of the above functions - output two csv files if specified (`to_csv=True`)
  - `csv_to_sql` collect a csv and load it into an sql table

### `leveraging_sql_dag.py`
This one is another aproach. It leverage mostly sql requests to do the job.
- `load_data` load csv as DataFrame, do some cleaning (see `load_clean_data`) and load the result into an sql table `reimbursements`
- `sql_aggregation` execute a sql query.
Here we compute results (average and repartition) directly in the postgresql instance (see `requests/alternate_aggregations/avg_amount_by_speciality.sql` and `requests/alternate_aggregations/amount_repartition_by_specilaity.sql` for more information).

## What would i do (with more time)?:
- Add tests on sql requests and sanity checks
- Add more test on DAGs such as test runs
- Add some sort of messaging on sanity check failling

## What could have i done differently?:
- Use PythonOperator with raising conditions, in place of the ShortCircuitOperator, in the sanity checks to fail the task, advantage of the ShortCircuitOperator is that it will skip the branch, computations on another branch will still happen + wanted to have fun.
- Using Xcom could have been great, but given the significative size of the dataframes I don't find it to be a very efficient way to do it.
- Could provide option to drop or not pre-existing tables (drop by default).
- Could provide a way to select which columns we want to keep from the source csv
<br>(The last to are kind of over-engineering the solution)
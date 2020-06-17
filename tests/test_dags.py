import os

import pytest
from airflow.models import DagBag


@pytest.fixture(scope="module")
def dag_bag():
  """
  TODO: Need better solution for AIRFLOW_HOME
  """
  dag_folder = os.getenv("AIRFLOW_HOME", "./")
  dag_bag = DagBag(dag_folder=dag_folder)

  return dag_bag


class TestDataAggregations():

  def test_task_count(self, dag_bag):
    dag_id = "data_aggregations"
    dag = dag_bag.get_dag(dag_id)
    assert len(dag.tasks) == 6

  def test_task_ids(self, dag_bag):
    dag_id = 'data_aggregations'
    dag = dag_bag.get_dag(dag_id)
    tasks = dag.tasks
    task_ids = sorted([task.task_id for task in tasks])
    assert task_ids == sorted([
      "csv_sanity_check", 
      "aggregate_data", 
      "avg_to_psql", 
      "repartition_to_psql", 
      "check_avg_to_psql", 
      "check_repartition_to_psql"
    ])

  @pytest.mark.parametrize(
    "task_id, upstream, downstream",[
      ("csv_sanity_check", [],["aggregate_data"]),
      ("aggregate_data", ["csv_sanity_check"], ["avg_to_psql", "repartition_to_psql"]),
      ("avg_to_psql", ["aggregate_data"], ["check_avg_to_psql"]),
      ("repartition_to_psql", ["aggregate_data"], ["check_repartition_to_psql"]),
      ("check_avg_to_psql", ["avg_to_psql"], []),
      ("check_repartition_to_psql", ["repartition_to_psql"], [])
    ]
  )
  def test_aggregate_task_dependencies(self, dag_bag, task_id, upstream, downstream):
    dag_id=dag_id = 'data_aggregations'
    dag = dag_bag.get_dag(dag_id)
    sel_task = dag.get_task(task_id)

    upstream_task_ids = [task.task_id for task in sel_task.upstream_list]
    assert sorted(upstream_task_ids) == sorted(upstream)

    downstream_tasks_ids = [task.task_id for task in sel_task.downstream_list]
    assert sorted(downstream_tasks_ids) == sorted(downstream)


class TestAlternateDataAggregations():

  def test_task_count(self, dag_bag):
    dag_id = "alternate_data_aggregations"
    dag = dag_bag.get_dag(dag_id)
    assert len(dag.tasks) == 7

  def test_task_ids(self, dag_bag):
    dag_id = 'alternate_data_aggregations'
    dag = dag_bag.get_dag(dag_id)
    tasks = dag.tasks
    task_ids = sorted([task.task_id for task in tasks])
    assert task_ids == sorted([
      "csv_sanity_check", 
      "load_data", 
      "reimbursement_sanity_check", 
      "compute_avg_by_speciality", 
      "check_avg_to_psql", 
      "compute_repartition_by_speciality",
      "check_repartition_to_psql"
    ])

  @pytest.mark.parametrize(
    "task_id, upstream, downstream",[
      ("csv_sanity_check", [],["load_data"]),
      ("load_data", ["csv_sanity_check"], ["reimbursement_sanity_check"]),
      ("reimbursement_sanity_check", ["load_data"], ["compute_avg_by_speciality", "compute_repartition_by_speciality"]),
      ("compute_avg_by_speciality", ["reimbursement_sanity_check"], ["check_avg_to_psql"]),
      ("compute_repartition_by_speciality", ["reimbursement_sanity_check"], ["check_repartition_to_psql"]),
      ("check_avg_to_psql", ["compute_avg_by_speciality"], []),
      ("check_repartition_to_psql", ["compute_repartition_by_speciality"], [])
    ]
  )
  def test_aggregate_task_dependencies(self, dag_bag, task_id, upstream, downstream):
    dag_id=dag_id = 'alternate_data_aggregations'
    dag = dag_bag.get_dag(dag_id)
    sel_task = dag.get_task(task_id)

    upstream_task_ids = [task.task_id for task in sel_task.upstream_list]
    assert sorted(upstream_task_ids) == sorted(upstream)

    downstream_tasks_ids = [task.task_id for task in sel_task.downstream_list]
    assert sorted(downstream_tasks_ids) == sorted(downstream)
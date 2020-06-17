import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


reimbursements_test_cases = [
  {
    "test_id": "test_not_empty_reimbusements",
    "test_description": "test if reimbursements table is empty",
    "test_request": """
      SELECT * 
      FROM reimbursements_201807
      LIMIT 1;
    """
  },
  {
    "test_id": "test_magnitude_reimbusement",
    "test_description": "test order of magnitude for reimbursements row number",
    "test_request": """
      SELECT COUNT(*) > 100000
      FROM reimbursements_201807;
    """
  }
]


repartition_test_cases = [
  {
    "test_id": "test_not_empty_repartition",
    "test_description": "test if total_amount_repartition_by_speciality table is empty",
    "test_request": """
      SELECT * 
      FROM total_amount_repartition_by_speciality
      LIMIT 1;
    """
  },
  {
    "test_id": "",
    "test_description": "",
    "test_request": """
      SELECT ROUND(SUM(total_amount_portion)) = 100 
      FROM total_amount_repartition_by_speciality;
    """
  }
]


avg_test_cases = [
  {
    "test_id": "test_not_empty_repartition",
    "test_description": "test if total_amount_repartition_by_speciality table is empty",
    "test_request": """
      SELECT * 
      FROM average_amount_by_speciality
      LIMIT 1;
    """
  }
]


def sql_sanity_checks(test_cases, session, on_failure_raise=False):
  for test_case in test_cases:
    df = pd.read_sql(test_case["test_request"], session.bind)
    status = True
    if df is None or df.shape[0] != 1:
      status &= False

    if False in df.iloc[0].values:
      status &= False

  if on_failure_raise:
    raise Exception
  else:
    return True
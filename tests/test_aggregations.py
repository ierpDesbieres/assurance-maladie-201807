import os

import pandas as pd
import pytest

from dags.data_aggregations.aggregations import load_clean_data, compute_average_by_speciality, compute_repartition_by_speciality, compute_total_by_speciality


MOCK_CSV = os.path.join(
  os.getenv("TESTS_DIR", "./"),
  "tests/mock.csv"
)
MOCK_CSV_ARGS = {"sep": ";", "encoding": "utf-8"}


MOCK_DF = (
  pd.DataFrame(
    [
      {"speciality": "médecine générale", "reimbursement_amount": 30.00},
      {"speciality": "médecine générale", "reimbursement_amount": 25.00},
      {"speciality": "neurologie", "reimbursement_amount": 45.10}
    ]
  )
  .set_index("speciality")
)
EXPECTED_AVG_DF = (
  pd.DataFrame(
    [
      {"speciality": "médecine générale", "reimbursement_amount": 27.50},
      {"speciality": "neurologie", "reimbursement_amount": 45.10}
    ]
  )
  .set_index("speciality")
)
EXPECTED_REPARTITION_DF = (
  pd.DataFrame(
    [
      {"speciality": "médecine générale", "reimbursement_amount": (30.00+25.00)/100.10*100},
      {"speciality": "neurologie", "reimbursement_amount": 45.10/100.10*100}
    ]
  )
  .set_index("speciality")
) 
EXPECTED_TOTAL_DF = (
  pd.DataFrame(
    [
      {"speciality": "médecine générale", "reimbursement_amount": 55.00},
      {"speciality": "neurologie", "reimbursement_amount": 45.10}
    ]
  )
  .set_index("speciality")
) 


def test_load_clean_data():
  df = load_clean_data(MOCK_CSV, MOCK_CSV_ARGS)
  pd.testing.assert_frame_equal(df, MOCK_DF)


def test_total_by_speciality():
  tot_df = compute_total_by_speciality(MOCK_DF)
  pd.testing.assert_frame_equal(tot_df, EXPECTED_TOTAL_DF)


def test_avg_amnt_by_speciality():
  avg_df = compute_average_by_speciality(MOCK_DF)
  pd.testing.assert_frame_equal(avg_df, EXPECTED_AVG_DF)


def test_repatition_by_speciality():
  repartition_df = compute_repartition_by_speciality(MOCK_DF)
  pd.testing.assert_frame_equal(repartition_df, EXPECTED_REPARTITION_DF)
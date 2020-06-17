import os

import pandas as pd
from sqlalchemy.orm.session import Session

from .config import config


def load_clean_data(csv: str, csv_args: dict, skip_negatives: bool=False) -> pd.DataFrame:
  """
  Load data from the csv file, clean it and output a pandas'DataFrame

  Args:
    csv (str):
      csv path
    csv_args (dict):
      arguments for pandas to properly parse the csv file (standard pd.read_csv arguments)
    skip_negatives (bool=False): 
      precise if you want to keep negative amounts (regularization of previous months)

  Returns:
    pd.DataFrame

  """
  
  _df = pd.read_csv(csv, **csv_args)
  df = _df[["l_exe_spe", "rem_mon"]]

  # Convert rem_mon from str to float
  df["rem_mon"] = df["rem_mon"].apply(lambda x: x.replace(".", "").replace(",", ".")).astype(float)
  
  # `rem_mon` of 0 is just an empty class we can discard it (no record on this month)
  # We could skip negatives as they are regularization of past month also
  query = "rem_mon != 0"
  if skip_negatives:
    query = "rem_mon > 0"
  df = df.query(query)

  df = df.rename(columns={"l_exe_spe": "speciality", "rem_mon": "reimbursement_amount"})

  return df.set_index("speciality")


def compute_total_by_speciality(df: pd.DataFrame) -> pd.DataFrame:
  """
  Copute the reimbursement total by speciality

  Args:
    df (pd.DataFrame):
      the source DataFrame from `load_clean_data`
  Returns:
    pd.DataFrame

  """
  total_df = df.groupby("speciality").sum()
  return total_df


def compute_average_by_speciality(df: pd.DataFrame) -> pd.DataFrame:
  """
  Compute the reimbursement average by speciality

  Args:
    df (pd.DataFrame):
      the source DataFrame from `load_clean_data`

  Returns:
    pd.DataFrame

  """
  avg_df = df.groupby("speciality").mean()
  return avg_df


def compute_repartition_by_speciality(df: pd.DataFrame) -> pd.DataFrame:
  """
  Compute the reimbursement amount repartition amongs specialities (% of the total amount)

  Args:
    df (pd.DataFrame):
      the source DataFrame from `load_clean_data`
  Returns:
    pd.DataFrame

  """
  
  tot_df = compute_total_by_speciality(df)
  repartition_df = (tot_df / tot_df.sum() * 100)
  return repartition_df


def insert_df_in_db(
  df: pd.DataFrame,
  session: Session,
  tablename: str,
  create_request: str
) -> None:

  tablename = table_config["tablename"]
  create_request = table_config["create_request"]

  # Create table schema
  session.execute(create_request)
  session.commit()

  # Insert data in the newly created schema
  df.to_sql(tablename, session.bind, if_exists="append")


def aggregate(csv: str, csv_args: dict, to_csv: bool=False):
  """
  Execute both aggregations (average and repartition)

  Args:
    csv (str):
      path to the csv file
    csv_args (dict):
      arguments for pandas to properly parse the csv file (standard pd.read_csv arguments)
    to_csv (bool=False):
      set argument to true if you want to save the result in config.datadir

  """
  
  df = load_clean_data(csv, csv_args)
  avg_df = compute_average_by_speciality(df)
  repartition_df = compute_repartition_by_speciality(df)

  if to_csv:
    os.makedirs(config.data_dir, exist_ok=True)
    avg_df.to_csv(os.path.join(config.data_dir, "avg_by_speciality.csv"))
    repartition_df.to_csv(os.path.join(config.data_dir, "repartition_by_speciality.csv"))
    return

  return avg_df, repartition_df


def csv_to_sql(csv_path: str, request_path: str, session: Session) -> None:
  """
  Load a csv file into a sql table

  Args:
    csv_path (str):
      path to the csv file
    request_path (str):
      path to the sql request
    session (Session):
      database session

  Returns:
    None

  """
  if not csv_path.startswith("/"):
    csv_path = os.path.abspath(csv_path)

  with open(request_path, "r") as f:
    _request = f.read()

  request = _request.format(csv_path=csv_path)
  session.execute(request)
  session.commit()
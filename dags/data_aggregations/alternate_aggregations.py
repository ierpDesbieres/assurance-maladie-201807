import os

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .config import config
from .aggregations import load_clean_data


def load_data(csv, csv_args, session):
  print(">> Loading source...")
  df = load_clean_data(csv, csv_args)
  df.to_sql("reimbursements_201807", session.bind, if_exists="replace")
  print(">> Done...")


def sql_aggregation(request, session):
  with open(request, "r") as f:
    req = f.read()
  session.execute(req)
  session.commit()
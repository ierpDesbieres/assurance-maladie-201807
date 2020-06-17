import os

class config():
  
  # Data directory
  data_dir = os.getenv(
    "DATA_DIR",
    os.path.abspath("./data/")
  )

  # Path for the csv source file
  csv_path = os.getenv(
    "CSV_PATH",
    "data/n201807.csv"
  )

  #Path for the sql files
  requests_path = os.getenv("REQ_DIR", "requests/")

  # Arguments for the data.gouv csv source file
  csv_args = {
    "sep": ";",
    "encoding": "cp1252"
  }

  # URL for the database
  db_url = os.getenv(
    "DB_URL",
    "postgresql://postgres:postgres@127.0.0.1:5432/reimbursements"
  )
import pandas as pd


def csv_test_cases(source_csv, csv_args):
  df = pd.read_csv(source_csv, **csv_args)
  test_cases = [
    {
      "test_id": "lines and columns",
      "test_description": "test if csv is not empty and has the right amount of colums",
      "test": (
        df.shape[0] > 0 and df.shape[1] == 28
      ),
      "fallback_conditions": [
        {
          "test_id": "needed columns",
          "test_description": "test if csv has columns of interest",
          "test": all(item in df.columns for item in ["l_exe_spe", "rem_mon"])
        }
      ]
    },
    {
      "test_id": "lines",
      "test_description": "test if csv is not empty",
      "test": (
        df.shape[0] != 0
      )
    },
    {
      "test_id": "columns",
      "test_description": "test if csv has the right amount of columns",
      "test": (
        df.shape[1] == 28
      )
    },
  ]

  return test_cases


def df_sanity_check(test_cases):
  for test_case in test_cases:
    status = check_test(test_case)
    if status is False:
      return status

  return True


def check_test(test_case: dict, on_failure_raise=False):
  """
  Check the status od a test and its fallback methods 
  (each fallback has to be True for the result to be valid)

  Args:
    test_case (dict):
      dictionnary of test cases

  """
  
  status = test_case.get("test")
  fallback_conditions = test_case.get("fallback_conditions")
  print(f"{test_case['test_id']} - {status}")

  if status is False and fallback_conditions:
    status = True
    for fallback_condition in fallback_conditions:
      status &= check_test(fallback_condition)

  return status
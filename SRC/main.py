import pandas as pd
import os
from sqlalchemy import create_engine
from utils.database_utils import test_database_connection
from utils.data_analysis import (
    load_data,
    get_counts,
    analyse_join_counts,
)

db_path = os.path.join("../data", "case.db")
# test db
test_database_connection(db_path)

# --- Data Analysis ---
cases_df = load_data(db_path, "cases")

# get counts
if cases_df is not None:
    get_counts(cases_df, "Origin")
    get_counts(cases_df, "Status")
    get_counts(cases_df, "Issue type")

# join counts
analyse_join_counts(db_path)

import pandas as pd
import os
from sqlalchemy import create_engine
from utils.database_utils import test_database_connection
from utils.data_analysis import (
    load_data,
    get_counts,
    analyse_join_counts,
    analyse_avg_handle_time,
    analyse_avg_phone_entries,
    analyse_avg_handle_time_by_issue_type,
    analyse_handle_time_issue_origin_counts,
    analyse_whatsapp_success_rate,
)

db_path = os.path.join("../data", "case.db")
# test db
test_database_connection(db_path)

# --- Data Analysis ---
cases_df = load_data(db_path, "cases")
phone_df = load_data(db_path, "phone")
omni_df = load_data(db_path, "email_web_whatsapp_community")
whatsapp_df = load_data(db_path, "whatsapp")

# get counts
if cases_df is not None:
    get_counts(cases_df, "Origin")
    get_counts(cases_df, "Status")
    get_counts(cases_df, "Issue type")

# join counts
analyse_join_counts(db_path)

# average handle time
analyse_avg_handle_time(cases_df, omni_df, phone_df)

# multiple call average
analyse_avg_phone_entries(cases_df, phone_df)

# average handle time per issue type
analyse_avg_handle_time_by_issue_type(cases_df, omni_df, phone_df)

# average handle time and counts per issue type and origin
analyse_handle_time_issue_origin_counts(cases_df, omni_df, phone_df)

# bot success rate so far
analyse_whatsapp_success_rate(whatsapp_df)

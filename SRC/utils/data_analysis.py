import pandas as pd
from sqlalchemy import create_engine
import os


def load_data(db_path, table_name, limit=None):
    """
    Loads data from SQLite database into Pandas df.

    :param db_path: The path to the SQLite database file.
    :type db_path: str
    :param table_name: The name of the table.
    :type table_name: str
    :param limit: The number of rows to load, defaults to None (load all).
    :type limit: int or None
    :return: The loaded data as a Pandas DataFrame, or None if an error occurs
    :rtype: pandas.DataFrame
    """
    database_url = f"sqlite:///{db_path}"
    engine = create_engine(database_url)
    try:
        query = f"SELECT * FROM {table_name}"
        if limit is not None:
            query += f" LIMIT {limit}"
        df = pd.read_sql(query, engine)
        return df
    except Exception as e:
        print(f"Error loading data from {table_name}: {e}")
        return None
    finally:
        engine.dispose()


def get_counts(df, column_name):
    """Calculates and prints value counts and percentage total for a specified column and saves to csv.

    :param df: pandas dataframe
    :param column_name: name of the column to be counted
    :return: count of the values of the column and percentage of total
    """
    if df is not None and column_name in df.columns:
        counts = df[column_name].value_counts()
        total = len(df)
        percentages = (counts / total) * 100
        output_path = os.path.join(
            "../data", f'{column_name.lower().replace(" ", "_")}_counts.csv'
        )
        counts_df = pd.DataFrame({"Count": counts, "Percentage": percentages})
        try:
            counts_df.to_csv(output_path)
            print(f"\nCounts and percentages of {column_name} saved to: {output_path}")
        except Exception as e:
            print(f"Error saving {column_name} counts to CSV: {e}")
        return counts, percentages
    else:
        print(f"Error: DataFrame is None or missing '{column_name}' column.")
        return None, None


def time_to_seconds(time_str):
    """
    Converts time to total seconds.
    :param time_str: time as a string HH:MM:SS
    :return: total seconds
    """
    if isinstance(time_str, str):
        if time_str == "-":
            return None
        parts = time_str.split(":")
        if len(parts) == 3:
            return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
        elif len(parts) == 2:
            return int(parts[0]) * 60 + int(parts[1])
        return int(parts[0])
    elif isinstance(time_str, (float, int)):
        return time_str
    return None


def analyse_avg_handle_time(cases_df, omni_df, phone_df):
    """
    Calculates and saves as CSV the average handle time per origin and status.
    :param cases_df: data frame of the cases table
    :param omni_df: data frame of the salesforce table
    :param phone_df: data frame of the phone call table
    :return: CSV of average time to handle by origin and by status.
    """
    if cases_df is not None and omni_df is not None and phone_df is not None:
        omni_df["Handle Time Seconds"] = omni_df["Handle Time"].fillna(
            0
        )  # Fill NaN with 0 for aggregation
        omni_handle_time_origin = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        )
        avg_handle_time_origin_omni = (
            omni_handle_time_origin.groupby("Origin")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_origin_omni["Source"] = "Omni"

        avg_handle_time_status_omni = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        )
        avg_handle_time_status_omni = (
            avg_handle_time_status_omni.groupby("Status_x")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_status_omni["Source"] = "Omni"
        avg_handle_time_status_omni = avg_handle_time_status_omni.rename(
            columns={"Status_x": "Status"}
        )

        # Prepare handle time in seconds for phone data
        phone_df["Handle Time Seconds"] = (
            phone_df["HANDLE TIME"].apply(time_to_seconds).fillna(0)
        )  # Fill NaN with 0
        phone_handle_time_origin = pd.merge(
            cases_df, phone_df, on="SESSION ID", how="inner"
        )
        avg_handle_time_origin_phone = (
            phone_handle_time_origin.groupby("Origin")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_origin_phone["Source"] = "Phone"

        avg_handle_time_status_phone = pd.merge(
            cases_df, phone_df, on="SESSION ID", how="inner"
        )
        avg_handle_time_status_phone = (
            avg_handle_time_status_phone.groupby("Status")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_status_phone["Source"] = "Phone"

        # Combine results
        avg_handle_time_origin = pd.concat(
            [avg_handle_time_origin_omni, avg_handle_time_origin_phone]
        )
        avg_handle_time_status = pd.concat(
            [avg_handle_time_status_omni, avg_handle_time_status_phone]
        )

        # Sort by average handle time (longest to shortest)
        avg_handle_time_origin_sorted = avg_handle_time_origin.sort_values(
            by="Handle Time Seconds", ascending=False
        )
        avg_handle_time_status_sorted = avg_handle_time_status.sort_values(
            by="Handle Time Seconds", ascending=False
        )

        # Save average handle time per origin to CSV
        output_path_origin = os.path.join("../data", "avg_handle_time_per_origin.csv")
        try:
            avg_handle_time_origin_sorted.to_csv(output_path_origin, index=False)
            print(
                f"\nAverage handle time per origin (sorted) saved to: {output_path_origin}"
            )
        except Exception as e:
            print(f"Error saving average handle time per origin to CSV: {e}")

        # Save average handle time per status to CSV
        output_path_status = os.path.join("../data", "avg_handle_time_per_status.csv")
        try:
            avg_handle_time_status_sorted.to_csv(output_path_status, index=False)
            print(
                f"Average handle time per status (sorted) saved to: {output_path_status}"
            )
        except Exception as e:
            print(f"Error saving average handle time per status to CSV: {e}")

    else:
        print(
            "Error: One or more of the required DataFrames (cases_df, omni_df, phone_df) are None."
        )


def analyse_join_counts(db_path):
    """
    Analyses how many rows in the 'cases' table have joins with other tables.
    So we can see if multiple channels are used in a singular case and the volume.
    """
    database_url = f"sqlite:///{db_path}"
    engine = create_engine(database_url)
    join_results = {}
    try:
        cases_df = pd.read_sql(
            'SELECT "Id", "Case Number", "Origin", "Status", "SESSION ID" FROM cases',
            engine,
        )
        phone_df = pd.read_sql('SELECT "SESSION ID" FROM phone', engine)
        omni_df = pd.read_sql(
            'SELECT "Work Item Id", "Handle Time" FROM email_web_whatsapp_community',
            engine,
        )
        whatsapp_df = pd.read_sql('SELECT "Case Id" FROM whatsapp', engine)

        # Phone to Case join count
        phone_join_df = pd.merge(cases_df, phone_df, on="SESSION ID", how="inner")
        phone_join_count = phone_join_df.shape[0]
        join_results["phone_to_case_join_count"] = phone_join_count
        print(
            f"\nNumber of cases joined with 'phone' table (via SESSION ID): {phone_join_count}"
        )

        # Salesforce Omni channel to Case join count
        omni_join_df = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        )
        omni_join_count = omni_join_df.shape[0]
        join_results["omni_to_case_join_count"] = omni_join_count
        print(
            f"Number of cases joined with 'email_web_whatsapp_community' table (via Id - Work Item Id): {omni_join_count}"
        )

        # Case and WhatsApp join count
        whatsapp_join_df = pd.merge(
            cases_df, whatsapp_df, left_on="Id", right_on="Case Id", how="inner"
        )
        whatsapp_join_count = whatsapp_join_df.shape[0]
        join_results["whatsapp_to_case_join_count"] = whatsapp_join_count
        print(
            f"Number of cases joined with 'whatsapp' table (via Id - Case Id): {whatsapp_join_count}"
        )

        # Cases with multiple joins
        cases_with_joins = pd.DataFrame(
            {
                "CaseId": cases_df["Id"],
                "HasPhoneJoin": cases_df["SESSION ID"].isin(phone_df["SESSION ID"]),
                "HasOmniJoin": cases_df["Id"].isin(omni_df["Work Item Id"]),
                "HasWhatsappJoin": cases_df["Id"].isin(whatsapp_df["Case Id"]),
            }
        )
        multiple_joins_count = cases_with_joins[
            cases_with_joins[["HasPhoneJoin", "HasOmniJoin", "HasWhatsappJoin"]].sum(
                axis=1
            )
            > 1
        ].shape[0]
        join_results["multiple_joins_count"] = multiple_joins_count
        print(
            f"\nNumber of cases with joins to more than one other table: {multiple_joins_count}"
        )

        # Cases with multiple entries in phone table
        cases_with_multiple_phone_entries = pd.merge(
            cases_df,
            phone_df.groupby("SESSION ID")
            .size()
            .reset_index(name="PhoneEntryCount")
            .query("PhoneEntryCount > 1"),
            on="SESSION ID",
            how="inner",
        ).shape[0]
        join_results["multiple_phone_entries_count"] = cases_with_multiple_phone_entries
        print(
            f"Number of cases with multiple entries in the 'phone' table: {cases_with_multiple_phone_entries}"
        )

        # Cases with multiple entries in omni table
        cases_with_multiple_omni_entries = pd.merge(
            cases_df,
            omni_df.groupby("Work Item Id")
            .size()
            .reset_index(name="OmniEntryCount")
            .query("OmniEntryCount > 1"),
            left_on="Id",
            right_on="Work Item Id",
            how="inner",
        ).shape[0]
        join_results["multiple_omni_entries_count"] = cases_with_multiple_omni_entries
        print(
            f"Number of cases with multiple entries in the 'email_web_whatsapp_community' table: {cases_with_multiple_omni_entries}"
        )

        # Cases with multiple entries in whatsapp table
        cases_with_multiple_whatsapp_entries = pd.merge(
            cases_df,
            whatsapp_df.groupby("Case Id")
            .size()
            .reset_index(name="WhatsappEntryCount")
            .query("WhatsappEntryCount > 1"),
            left_on="Id",
            right_on="Case Id",
            how="inner",
        ).shape[0]
        join_results["multiple_whatsapp_entries_count"] = (
            cases_with_multiple_whatsapp_entries
        )
        print(
            f"Number of cases with multiple entries in the 'whatsapp' table: {cases_with_multiple_whatsapp_entries}"
        )

        # Save join results to CSV
        output_path = os.path.join("../data", "join_analysis_results.csv")
        join_results_df = pd.DataFrame(
            list(join_results.items()), columns=["Metric", "Count"]
        )
        try:
            join_results_df.to_csv(output_path, index=False)
            print(f"\nJoin analysis results saved to: {output_path}")
        except Exception as e:
            print(f"Error saving join analysis results to CSV: {e}")

    except Exception as e:
        print(f"Error during join analysis: {e}")
    finally:
        engine.dispose()


def analyse_avg_phone_entries(cases_df, phone_df):
    """
    Calculates and saves to CSV the average number of phone entries per case (overall and for cases with >1 call).
    :param cases_df: cases dataframe
    :param phone_df: phone call dataframe
    :return: results data frame and saves to csv
    """
    if cases_df is not None and phone_df is not None:
        # Join cases and phone tables
        cases_phone_joined = pd.merge(cases_df, phone_df, on="SESSION ID", how="inner")

        # Count the number of phone entries per case
        phone_entries_per_case = cases_phone_joined.groupby("Id")["SESSION ID"].count()

        analysis_results = {}

        # Average number of phone entries per case (all cases with phone interaction)
        if not phone_entries_per_case.empty:
            average_phone_entries_per_case = phone_entries_per_case.mean()
            analysis_results["avg_phone_entries_per_case"] = (
                average_phone_entries_per_case
            )
            print(
                f"\nAverage number of phone call entries per case handled by phone: {average_phone_entries_per_case:.2f}"
            )
        else:
            analysis_results["avg_phone_entries_per_case"] = 0
            print(
                "\nNo cases were handled by phone to calculate the average number of phone entries."
            )

        # Average number of phone entries for cases with more than one call
        multiple_phone_calls = phone_entries_per_case[phone_entries_per_case > 1]
        if not multiple_phone_calls.empty:
            average_phone_entries_multiple_calls = multiple_phone_calls.mean()
            analysis_results["avg_phone_entries_gt_one_call"] = (
                average_phone_entries_multiple_calls
            )
            print(
                f"Average number of phone call entries for cases with more than one call: {average_phone_entries_multiple_calls:.2f}"
            )
        else:
            analysis_results["avg_phone_entries_gt_one_call"] = 0
            print(
                "\nNo cases required more than one phone call to calculate the average number of calls."
            )

        # Save to CSV
        output_path = os.path.join("../data", "avg_phone_entries_analysis.csv")
        results_df = pd.DataFrame(
            list(analysis_results.items()), columns=["Metric", "Average"]
        )
        try:
            results_df.to_csv(output_path, index=False)
            print(f"\nAverage phone entries analysis saved to: {output_path}")
        except Exception as e:
            print(f"Error saving average phone entries analysis to CSV: {e}")

    else:
        print("Error: cases_df or phone_df is None.")


def analyse_avg_handle_time_by_issue_type(cases_df, omni_df, phone_df):
    """
    Calculates and saves as CSV the average handle time per issue type.
    :param cases_df: data frame of the cases table (must contain 'Issue type' and 'Id' columns)
    :param omni_df: data frame of the salesforce table (must contain 'Work Item Id' and 'Handle Time' columns)
    :param phone_df: data frame of the phone call table (must contain 'SESSION ID' and 'HANDLE TIME' columns)
    :return: None
    """
    if (
        cases_df is not None
        and omni_df is not None
        and phone_df is not None
        and "Issue type" in cases_df.columns
    ):
        # Prepare handle time in seconds for omni data
        omni_df["Handle Time Seconds"] = (
            omni_df["Handle Time"]
            .apply(lambda x: time_to_seconds(x) if pd.notna(x) else None)
            .fillna(0)
        )
        omni_issue_handle_time = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        )
        avg_handle_time_issue_omni = (
            omni_issue_handle_time.groupby("Issue type")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_issue_omni["Source"] = "Omni"

        # Prepare handle time in seconds for phone data
        phone_df["Handle Time Seconds"] = (
            phone_df["HANDLE TIME"]
            .apply(lambda x: time_to_seconds(x) if pd.notna(x) else None)
            .fillna(0)
        )
        phone_issue_handle_time = pd.merge(
            cases_df, phone_df, left_on="SESSION ID", right_on="SESSION ID", how="inner"
        )
        avg_handle_time_issue_phone = (
            phone_issue_handle_time.groupby("Issue type")["Handle Time Seconds"]
            .mean()
            .reset_index()
        )
        avg_handle_time_issue_phone["Source"] = "Phone"

        # Combine results
        avg_handle_time_issue = pd.concat(
            [avg_handle_time_issue_omni, avg_handle_time_issue_phone]
        )

        # Sort by average handle time (longest to shortest)
        avg_handle_time_issue_sorted = avg_handle_time_issue.sort_values(
            by="Handle Time Seconds", ascending=False
        )

        # Save average handle time per issue type to CSV
        output_path = os.path.join("../data", "avg_handle_time_per_issue_type.csv")
        try:
            avg_handle_time_issue_sorted.to_csv(output_path, index=False)
            print(
                f"\nAverage handle time per issue type (sorted) saved to: {output_path}"
            )
        except Exception as e:
            print(f"Error saving average handle time per issue type to CSV: {e}")

    else:
        print(
            "Error: One or more of the required DataFrames are None or missing necessary columns for issue type analysis."
        )


def analyse_handle_time_issue_origin_counts(cases_df, omni_df, phone_df):
    """
    Calculates and saves as CSV the average handle time, counts per issue type and origin.
    :param cases_df: data frame of the cases table (must contain 'Issue type', 'Id', and 'Origin' columns)
    :param omni_df: data frame of the salesforce table (must contain 'Work Item Id' and 'Handle Time' columns)
    :param phone_df: data frame of the phone call table (must contain 'SESSION ID' and 'HANDLE TIME' columns)
    :param output_dir: directory to save the CSV file
    :return: None
    """
    if (
        cases_df is not None
        and omni_df is not None
        and phone_df is not None
        and "Issue type" in cases_df.columns
        and "Id" in cases_df.columns
        and "Origin" in cases_df.columns
    ):

        # Prepare handle time in seconds for omni data
        omni_df["Handle Time Seconds"] = (
            omni_df["Handle Time"]
            .apply(lambda x: time_to_seconds(x) if pd.notna(x) else None)
            .fillna(0)
        )
        omni_issue_origin_handle_time = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        )
        avg_handle_time_omni = (
            omni_issue_origin_handle_time.groupby(["Issue type", "Origin"])[
                "Handle Time Seconds"
            ]
            .mean()
            .reset_index()
        )
        avg_handle_time_omni["Source"] = "Omni"

        # Prepare handle time in seconds for phone data
        phone_df["Handle Time Seconds"] = (
            phone_df["HANDLE TIME"]
            .apply(lambda x: time_to_seconds(x) if pd.notna(x) else None)
            .fillna(0)
        )
        phone_issue_origin_handle_time = pd.merge(
            cases_df, phone_df, left_on="SESSION ID", right_on="SESSION ID", how="inner"
        )
        avg_handle_time_phone = (
            phone_issue_origin_handle_time.groupby(["Issue type", "Origin"])[
                "Handle Time Seconds"
            ]
            .mean()
            .reset_index()
        )
        avg_handle_time_phone["Source"] = "Phone"

        # Combine average handle times
        avg_handle_time = pd.concat([avg_handle_time_omni, avg_handle_time_phone])

        # Calculate counts per issue type and origin
        issue_origin_counts = (
            cases_df.groupby(["Issue type", "Origin"]).size().reset_index(name="Count")
        )

        # Merge average handle time and counts
        merged_df = pd.merge(
            avg_handle_time,
            issue_origin_counts,
            on=["Issue type", "Origin"],
            how="left",
        )

        # Sort by average handle time (longest to shortest)
        merged_df_sorted = merged_df.sort_values(
            by="Handle Time Seconds", ascending=False
        )

        # Save to CSV
        output_path = os.path.join("../data", "avg_handle_time_issue_origin_counts.csv")
        try:
            merged_df_sorted.to_csv(output_path, index=False)
            print(
                f"\nAverage handle time, counts per issue type and origin saved to: {output_path}"
            )
        except Exception as e:
            print(
                f"Error saving average handle time, counts per issue type and origin to CSV: {e}"
            )

    else:
        print(
            "Error: One or more of the required DataFrames are None or missing necessary columns for this analysis."
        )


def analyse_whatsapp_success_rate(whatsapp_df):
    """
    Calculates the success rate of bot vs. human agents in the provided whatsapp DataFrame
    based on whether the message count is greater than 0.

    :param whatsapp_df: pandas DataFrame of the whatsapp table.
    :return: None
    """
    if whatsapp_df is None:
        print("Error: whatsapp_df is None.")
        return

    if (
        "Agent Type" not in whatsapp_df.columns
        or "Agent Message Count" not in whatsapp_df.columns
    ):
        print(
            "Error: Both 'Agent Type' and 'Agent Message Count' columns are required in the whatsapp DataFrame."
        )
        return

    try:
        # Identify bot and human interactions (adjust logic based on your actual data)
        bot_interactions = whatsapp_df[whatsapp_df["Agent Type"] == "Bot"]
        human_interactions = whatsapp_df[whatsapp_df["Agent Type"] == "Agent"]

        # Calculate success rates based on message count > 0
        total_bot_interactions = len(bot_interactions)
        successful_bot_interactions = len(
            bot_interactions[bot_interactions["Agent Message Count"] > 0]
        )
        bot_success_rate = (
            (successful_bot_interactions / total_bot_interactions) * 100
            if total_bot_interactions > 0
            else 0
        )

        total_human_interactions = len(human_interactions)
        successful_human_interactions = len(
            human_interactions[human_interactions["Agent Message Count"] > 0]
        )
        human_success_rate = (
            (successful_human_interactions / total_human_interactions) * 100
            if total_human_interactions > 0
            else 0
        )

        # Create a summary DataFrame
        success_rate_df = pd.DataFrame(
            {
                "Agent Type": ["Bot", "Human"],
                "Total Interactions": [
                    total_bot_interactions,
                    total_human_interactions,
                ],
                "Successful Interactions (Message Count > 0)": [
                    successful_bot_interactions,
                    successful_human_interactions,
                ],
                "Success Rate (%)": [bot_success_rate, human_success_rate],
            }
        )

        # Save to CSV
        output_path = os.path.join("../data", "whatsapp_success_rate.csv")
        try:
            success_rate_df.to_csv(output_path, index=False)
            print(
                f"\nWhatsApp bot vs. human success rate analysis (based on message count > 0) saved to: {output_path}"
            )
        except Exception as e:
            print(f"Error saving WhatsApp success rate analysis to CSV: {e}")

    except Exception as e:
        print(f"Error during WhatsApp success rate analysis: {e}")

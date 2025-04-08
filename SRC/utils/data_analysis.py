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
        print(
            f"Error: DataFrame is None or missing '{column_name}' column for {title} analysis."
        )
        return None, None


def analyse_join_counts(db_path):
    """
    Analyses how many rows in the 'cases' table have joins with other tables.
    So we can see if multiple channels are used in a singular case and the volume.
    """
    database_url = f"sqlite:///{db_path}"
    engine = create_engine(database_url)
    join_results = {}
    try:
        cases_df = pd.read_sql('SELECT "Id", "SESSION ID" FROM cases', engine)
        phone_df = pd.read_sql('SELECT "SESSION ID" FROM phone', engine)
        omni_df = pd.read_sql(
            'SELECT "Work Item Id" FROM email_web_whatsapp_community', engine
        )
        whatsapp_df = pd.read_sql('SELECT "Case Id" FROM whatsapp', engine)

        # Phone to Case join count
        phone_join_count = pd.merge(
            cases_df, phone_df, on="SESSION ID", how="inner"
        ).shape[0]
        join_results["phone_to_case_join_count"] = phone_join_count
        print(
            f"\nNumber of cases joined with 'phone' table (via SESSION ID): {phone_join_count}"
        )

        # Salesforce Omni channel to Case join count
        omni_join_count = pd.merge(
            cases_df, omni_df, left_on="Id", right_on="Work Item Id", how="inner"
        ).shape[0]
        join_results["omni_to_case_join_count"] = omni_join_count
        print(
            f"Number of cases joined with 'email_web_whatsapp_community' table (via Id - Work Item Id): {omni_join_count}"
        )

        # Case and WhatsApp join count
        whatsapp_join_count = pd.merge(
            cases_df, whatsapp_df, left_on="Id", right_on="Case Id", how="inner"
        ).shape[0]
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

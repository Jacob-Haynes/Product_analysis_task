from sqlalchemy import create_engine
import pandas as pd
import os


def test_database_connection(db_path):
    """
    Tests the connection to the SQLite database and prints sample data from tables.

    :param db_path: The path to the SQLite database file.
    :type db_path: str
    """
    database_url = f"sqlite:///{db_path}"
    engine = create_engine(database_url)

    try:
        print("Testing database connection...")

        # Test 'cases' table
        query_cases = "SELECT * FROM cases LIMIT 2"
        cases_df = pd.read_sql(query_cases, engine)
        print("\nSample from 'cases' table:")
        print(cases_df)

        # Test 'phone' table join
        join_query_phone = """
        SELECT c."Case Number", c."Origin", p."CAMPAIGN", p."HANDLE TIME"
        FROM cases c
        LEFT JOIN phone p ON c."SESSION ID" = p."SESSION ID"
        LIMIT 2
        """
        phone_cases_df = pd.read_sql(join_query_phone, engine)
        print("\nSample from 'cases' joined with 'phone':")
        print(phone_cases_df)

        # Test 'email_web_whatsapp_community' table join
        join_query_omni = """
        SELECT c."Case Number", c."Status", e."Queue name", e."Handle Time"
        FROM cases c
        LEFT JOIN email_web_whatsapp_community e ON c."Id" = e."Work Item Id"
        LIMIT 2
        """
        omni_cases_df = pd.read_sql(join_query_omni, engine)
        print("\nSample from 'cases' joined with 'email_web_whatsapp_community':")
        print(omni_cases_df)

        # Test 'whatsapp' table join
        join_query_whatsapp = """
        SELECT c."Case Number", c."Issue type", w."Agent Type", w."Status"
        FROM cases c
        LEFT JOIN whatsapp w ON c."Id" = w."Case Id"
        LIMIT 2
        """
        whatsapp_cases_df = pd.read_sql(join_query_whatsapp, engine)
        print("\nSample from 'cases' joined with 'whatsapp':")
        print(whatsapp_cases_df)

        print("\nDatabase connection and basic queries successful.")

    except Exception as e:
        print(f"Error during database connection test: {e}")

    finally:
        engine.dispose()


if __name__ == "__main__":
    db_path_local = os.path.join("..", "data", "case.db")
    test_database_connection(db_path_local)

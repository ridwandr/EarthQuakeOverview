"""
load.py

Module to load cleaned earthquake data into Google BigQuery using service account authentication.

Author: Bayu Dwi Prasetya
Created: 2025-07-06

Dependencies:
- pandas
- pandas-gbq
- pyarrow
- google-auth
"""

import os
import pandas as pd
from pandas_gbq import to_gbq
from google.oauth2 import service_account
import logging

from prefect_gcp import GcpCredentials

logging.basicConfig(level=logging.INFO)

def upload_to_bigquery(
    df: pd.DataFrame,
    table_id: str,
    project_id: str,
    # credentials_path: str,
    if_exists: str = "replace"
):
    """
    Upload a pandas DataFrame to Google BigQuery using service account credentials.

    Parameters:
        df (pd.DataFrame): Cleaned and enriched data to upload.
        table_id (str): Full table path in BigQuery (e.g., dataset.table_name).
        project_id (str): Google Cloud project ID.
        credentials_path (str): Path to the service account JSON key file.
        if_exists (str): What to do if table exists. Options: 'fail', 'replace', 'append'.
                         Default is 'replace'.

    Returns:
        None
    """
    try:
        credentials = GcpCredentials.load("gcp-credentials").get_credentials_from_service_account()
        # logging.info(f"Authenticating using service account at: {credentials_path}")
        # credentials = service_account.Credentials.from_service_account_file(
        #     credentials_path
        # )

        logging.info(f"Uploading to BigQuery table: {table_id} (mode: {if_exists})")
        to_gbq(
            dataframe=df,
            destination_table=table_id,
            project_id=project_id,
            if_exists=if_exists,
            credentials=credentials
        )
        logging.info("Upload completed successfully.")
    except Exception as e:
        logging.error(f"Failed to upload to BigQuery: {e}")
# coding=utf-8
"""
# --------------------------------------------------------------------------------
# Licensed under the MIT LICENSE.
# See License.txt in the project root for license information.
# --------------------------------------------------------------------------------
"""
import os
import time
import datetime
import pytz
from typing import Tuple
import pandas as pd
import pygsheets
from sklearn.preprocessing import OrdinalEncoder
from apiclient import errors
from .constants import *


def get_pandas_df(spark, table_name: str) -> pd.DataFrame:
    """
    Load spark table and convert to pandas.
    """
    # Gets the latest Databricks table refresh
    spark.sql(f'refresh table {table_name}')

    # Load into a dataframe via spark and convert to pandas.
    df = spark.sql(f'select * from {table_name}')
    pdf = df.toPandas()
    return pdf


def split_dataframe(df: pd.DataFrame,
                    chunk_size: int = TABLEAU_MANAGEABLE_ROWS
                    ) -> list:
    """
    Splits dataframe into chunks where each chunk has a specified size (in rows).
    """
    chunks = list()
    num_chunks = len(df) // chunk_size + 1
    for i in range(num_chunks):
        chunks.append(df[i * chunk_size:(i + 1) * chunk_size])
    return chunks


def clean_google_sheets_name(name: str
                             ) -> str:
    """
    Cleans the google sheets name for clarity.
    """
    return name.replace('_csv', '').replace('events_', '').replace('_parquet', '')


def handle_google_sheets_auth():
    """
    Connect to google sheets via API, then uploads and overwrites worksheets by name.
    """
    gc = pygsheets.authorize(service_file=ETL_GOOGLE_SHEETS_CREDENTIALS)
    return gc


def delete_unused_google_sheets(num_chunks: int):
    """
    Connect to google sheets via API, then delete any old google sheets not currently used.
    """

    gc = handle_google_sheets_auth()

    # Try to open the Google sheet based on its title and if it fails, create it
    keep_titles = ['all_events_' + str(n) for n in range(num_chunks)]
    keep_titles.append('data_dictionary')

    spreadsheets = gc.spreadsheet_titles()
    delete_titles = [item for item in spreadsheets if item not in keep_titles and 'all_events_' in item]

    for sheet_title in delete_titles:
        try:
            print(f'Attempting to delete sheet {sheet_title}')
            wb = gc.open(sheet_title)
            gc.drive.delete(wb.id)
        except errors.HttpError as error:
            print(f'An error occurred on sheet {sheet_title}: {error}')


def handle_google_sheets(clean_table_name: str,
                         clean_table_name_chunk: str,
                         pdf_chunk: pd.DataFrame
                         ):
    """
    Connect to google sheets via API, then uploads and overwrites worksheets by name.
    """
    gc = handle_google_sheets_auth()

    # print for troubleshooting
    print(clean_table_name_chunk)

    # resize the sheet dynamically
    num_rows = len(pdf_chunk)
    num_cols = len(list(pdf_chunk.columns))

    # Try to open the Google sheet based on its title and if it fails, create it
    sheet_title = clean_table_name_chunk
    try:
        wb = gc.open(sheet_title)
        sheet = wb.worksheet_by_title(sheet_title)
        print(f"Opened spreadsheet with id:{sheet_title}")

    except pygsheets.SpreadsheetNotFound as error:
        # Can't find it and so create it
        res = gc.sheet.create(sheet_title)
        sheet_id = res['spreadsheetId']
        sheet = gc.open_by_key(sheet_id)
        print(f"Created spreadsheet with id:{sheet.id} and url:{sheet.url}")

        wb = gc.open(sheet_title)

        # Share with self to allow to write to it
        wb.share(ETL_ROBOT_EMAIL, role='writer', type='user')
        wb.share(ETL_USER_EMAIL, role='writer', type='user')

        # Share to all for reading
        wb.share('', role='reader', type='anyone')

        wb.add_worksheet(sheet_title,
                         rows=num_rows,
                         cols=num_cols)

        # open the sheet by name
        sheet = wb.worksheet_by_title(sheet_title)

    sheet.resize(num_rows, num_cols)

    # Write dataframe into it
    sheet.set_dataframe(pdf_chunk, (1, 1))


def add_event_name_column(pdf: pd.DataFrame, clean_table_name: str) -> pd.DataFrame:
    """
    Create a new column with the event name
    """
    pdf['event_name'] = [clean_table_name for _ in range(len(pdf))]
    return pdf


def add_unique_columns(pdf: pd.DataFrame,
                       unique_col_mapping: dict,
                       default_value_map: dict) -> dict:
    """
    Appends newly found unique columns to dictionary along with types.
    """
    for col in pdf.columns:
        if col not in unique_col_mapping:
            if col in default_value_map:
                unique_col_mapping[col] = default_value_map[col]
            else:
                unique_col_mapping[col] = np.nan
    return unique_col_mapping


def get_event_mapping(spark,
                      all_columns: list,
                      default_value_map: dict,
                      list_of_spark_tables: list,
                      unique_col_mapping=None,
                      combined_df=None) -> Tuple[dict, pd.DataFrame]:
    """
    Finds unique column names and their type for all event tables.
    """
    # Loops through each table.
    if unique_col_mapping is None:
        unique_col_mapping = {}
    if combined_df is None:
        combined_df = {}
    for table_name in list_of_spark_tables:
        # Cleans the google sheets name for clarity.
        clean_table_name = clean_google_sheets_name(table_name)
        pdf = get_pandas_df(spark, table_name)
        pdf = add_event_name_column(pdf, clean_table_name)
        unique_col_mapping = add_unique_columns(pdf, unique_col_mapping, default_value_map)

    unique_col_mapping_cp = {}
    for col in all_columns:
        unique_col_mapping_cp[col] = unique_col_mapping[col]

    for col in unique_col_mapping_cp:
        combined_df[col] = []

    return unique_col_mapping_cp, pd.DataFrame(combined_df)


def add_missing_columns(pdf: pd.DataFrame,
                        unique_col_mapping: dict,
                        all_columns: list) -> pd.DataFrame:
    """
    Creates a consistent set of columns and their types across all event tables.
    """
    # Create new columns with default missing values per the correct data type
    missing_cols = [col for col in all_columns if col not in pdf.columns]
    for col in missing_cols:
        default_value = unique_col_mapping[col]
        pdf[col] = [default_value for _ in range(len(pdf))]

    pdf = pdf[all_columns]
    return pdf


def concat_dataframes(pdf: pd.DataFrame, combined_df: pd.DataFrame):
    """
    Joins the event table to the combined events table.
    """
    combined_df = combined_df.reset_index().drop('index', axis=1)
    pdf = pdf.reset_index().drop('index', axis=1)

    # Concatenate the dataframes
    return pd.concat([combined_df, pdf])


def handle_types_and_missing_values(pdf: pd.DataFrame,
                                    default_value_map: dict,
                                    all_columns: list,
                                    type_map: dict,
                                    data_dictionary: pd.DataFrame
                                    ) -> pd.DataFrame:
    """
    Final cleanup to fill-in NA values before splitting the df.
    """

    for col in default_value_map:
        if col in pdf.columns:
            pdf[col] = pdf[col].replace([np.inf, -np.inf], np.nan)
            default_value = default_value_map[col]
            pdf[col] = pdf[col].fillna(default_value)
            col_type_longform = data_dictionary[data_dictionary['Column'] == col]['Type'].values[0]
            col_type = type_map[
                data_dictionary[data_dictionary['Column'] == col
                                ]['Type'].values[0]]['type']
            if col_type_longform == 'decimal':                # mike you had col_type here before which would not have been catching 'decimal'
                for err in REPLACE_WITH_NA:
                    pdf[col] = pdf[col].replace(err, default_value)
            elif col_type_longform == 'integer':
                pdf.loc[:,col] = [int(float(x)) for x in pdf.loc[:,col]]
            elif col_type_longform == 'datetime':
                pdf.loc[:,col] = [datetime.datetime.fromisoformat(str(x).replace('nan','1970-01-01')).astimezone(pytz.utc) for x in pdf.loc[:,col]]
            else:
                pass
            pdf.loc[:,col] = pdf.loc[:,col].astype(col_type)

    pdf['bntprice'] = pdf['bntprice'].replace('0E+18', '0.0')
    pdf['emaRate'] = pdf['emaRate'].replace('0E+18', '0.0')
    pdf['bntprice'] = pdf['bntprice'].replace('<NA>', '0.0')
    pdf['emaRate'] = pdf['emaRate'].replace('<NA>', '0.0')
    return pdf[all_columns]


def get_index_df(combined_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Creates a new df with all index values to rejoin in Tableau.
    """
    combined_df = combined_df.reset_index()
    combined_df = combined_df.drop('index', axis=1)
    combined_df = combined_df.reset_index()
    index_df = combined_df[['index']]
    return combined_df, index_df


def is_file_size_compatible(pdf_chunks: list,
                            is_file_size_compatible: bool = True,
                            filepath: str = '/dbfs/FileStore/tables/sizecheck.csv',
                            size_limit: int = 10000000
                            ) -> bool:
    """
    Checks that csv filesize < 10MB.
    """
    for chunk in pdf_chunks:
        chunk.to_csv(filepath, index=False)
        sz = os.path.getsize(filepath)
        if sz > size_limit:
            is_file_size_compatible = False
            break
    return is_file_size_compatible


def encode_address_columns(combined_df: pd.DataFrame, address_columns: list) -> pd.DataFrame:
    """
    Encodes the Ethereum address column values with an integer id to improve performance for tableau.
    """
    enc = OrdinalEncoder()
    X = combined_df[address_columns]
    X = X.fillna('NA')
    X_t = enc.fit_transform(X)
    combined_df[address_columns] = X_t
    return combined_df


def get_data_dictionary() -> pd.DataFrame:
    """
    Gets the latest data dictionary from google sheets and converts to a pandas dataframe.
    """
    gc = handle_google_sheets_auth()

    # open the sheet data dictionary by name
    wb = gc.open('data_dictionary')
    data_dictionary = wb.worksheet_by_title('data_dictionary').get_as_df()
    return data_dictionary


def remove_unused_events(combined_df: pd.DataFrame) -> pd.DataFrame:
    """
    Removes unused events from the combined dataframe.
    """
    keep_events = combined_df['event_name'].unique()
    unused_events = [clean_google_sheets_name(event) for event in UNUSED_EVENTS]
    keep_events = [
        clean_google_sheets_name(event) for event in keep_events if event not in unused_events
    ]
    combined_df = combined_df[
        combined_df['event_name'].isin(keep_events)
    ]
    return combined_df

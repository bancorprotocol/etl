# Databricks notebook source
# coding=utf-8
# --------------------------------------------------------------------------------
# MIT License
# Copyright (c) 2022 Bancor
# --------------------------------------------------------------------------------
"""
Instructions:

Adding new tables:
* Ensure a spark table exists with an appropriate table name
* Add the table name to the list of tables in CMD 5 in this notebook.

Adding new columns:
* Update the data dictionary with new column and type in google sheets:

Updates require 30+ minutes to complete once started.
"""

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

!cp ../requirements.txt ~/.
%pip install -r ~/requirements.txt

# COMMAND ----------

from bancor_etl.google_sheets_utils import *

# COMMAND ----------

ADDRESS_COLUMNS = [
    # 'contextId', 'pool', 'txhash', 'provider'
]

REPLACE_WITH_NA = ['0E+18', '<NA>']

# Maps the google sheets data dictionary to python/pandas types and fillna values
TYPE_MAP = {
    'decimal': {
        'type': str,
        'fillna': '0.0'
    },
    'integer': {
        'type': int,
        'fillna': 0
    },
    'string': {
        'type': str,
        'fillna': np.nan
    },
    'datetime': {
        'type': 'datetime64[ns]',
        'fillna': np.nan
    },
    'bool': {
        'type': bool,
        'fillna': np.nan
    },
}

LIST_OF_SPARK_TABLES = [
    # Add new table names here (see instructions at top of notebook)

    # NEW TABLES -> implemented on July 5, 2022
 'events_all_tokensdeposited_csv',
 'events_bancornetwork_flashloancompleted_csv',
 'events_bancornetwork_fundsmigrated_csv',
 'events_bancornetwork_networkfeeswithdrawn_csv',
 'events_bancornetwork_pooladded_csv',
 'events_bancornetwork_poolcollectionadded_csv',
 'events_bancornetwork_poolcollectionremoved_csv',
 'events_bancornetwork_poolcreated_csv',
 'events_bancornetwork_poolremoved_csv',
 'events_bancornetwork_tokenstraded_csv',
 'events_bancornetwork_tokenstraded_updated_csv',
 'events_bancorportal_sushiswappositionmigrated_csv',
 'events_bancorportal_uniswapv2positionmigrated_csv',
 'events_bancorv1migration_positionmigrated_csv',
 'events_bntpool_fundingrenounced_csv',
 'events_bntpool_fundingrequested_csv',
 'events_bntpool_fundsburned_csv',
 'events_bntpool_fundswithdrawn_csv',
 'events_bntpool_tokensdeposited_csv',
 'events_bntpool_tokenswithdrawn_csv',
 'events_bntpool_totalliquidityupdated_csv',
 'events_combined_tokenstraded_daily_fees_csv',
 'events_combined_tokenstraded_daily_volume_csv',
 'events_externalprotectionvault_fundsburned_csv',
 'events_externalprotectionvault_fundswithdrawn_csv',
 'events_externalrewardsvault_fundsburned_csv',
 'events_externalrewardsvault_fundswithdrawn_csv',
 'events_mastervault_fundsburned_csv',
 'events_mastervault_fundswithdrawn_csv',
 'events_networksettings_defaultflashloanfeeppmupdated_csv',
 'events_networksettings_flashloanfeeppmupdated_csv',
 'events_networksettings_fundinglimitupdated_csv',
 'events_networksettings_minliquidityfortradingupdated_csv',
 'events_networksettings_tokenaddedtowhitelist_csv',
 'events_networksettings_tokenremovedfromwhitelist_csv',
 'events_networksettings_vortexburnrewardupdated_csv',
 'events_networksettings_withdrawalfeeppmupdated_csv',
 'events_pendingwithdrawals_withdrawalcancelled_csv',
 'events_pendingwithdrawals_withdrawalcompleted_csv',
 'events_pendingwithdrawals_withdrawalcurrentpending_csv',
 'events_pendingwithdrawals_withdrawalinitiated_csv',
 'events_poolcollection_defaulttradingfeeppmupdated_csv',
 'events_poolcollection_depositingenabled_csv',
 'events_poolcollection_tokensdeposited_csv',
 'events_poolcollection_tokenswithdrawn_csv',
 'events_poolcollection_totalliquidityupdated_csv',
 'events_poolcollection_tradingenabled_csv',
 'events_poolcollection_tradingfeeppmupdated_csv',
 'events_poolcollection_tradingliquidityupdated_csv',
 'events_poolcollection_tradingliquidityupdated_spotrates_csv',
 'events_pooldata_historical_latest_csv',
 'events_stakingrewardsclaim_rewardsclaimed_csv',
 'events_stakingrewardsclaim_rewardsstaked_csv',
 'events_standardrewards_programcreated_csv',
 'events_standardrewards_programenabled_csv',
 'events_standardrewards_programterminated_csv',
 'events_standardrewards_providerjoined_csv',
 'events_standardrewards_providerleft_csv',
 'events_standardrewards_rewardsclaimed_csv',
 'events_standardrewards_rewardsstaked_csv',
 'events_trade_slippage_stats_csv',
 'events_v3_daily_bnttradingliquidity_csv',
 'events_v3_historical_deficit_by_tkn_csv',
 'events_v3_historical_deficit_csv',
 'events_v3_historical_spotrates_emarates_csv',
 'events_v3_historical_tradingliquidity_csv'
]

UNUSED_EVENTS = [
    'poolcollection_defaulttradingfeeppmupdated_csv',
    'events_poolcollection_depositingenabled_csv',
    'events_poolcollection_totalliquidityupdated_csv',
    'events_poolcollection_tradingfeeppmupdated_csv',
    'events_poolcollection_tradingliquidityupdated_csv',
    'events_stakingrewardsclaim_rewardsclaimed_csv',
    'events_stakingrewardsclaim_rewardsstaked_csv',
    'events_standardrewards_programcreated_csv',
    'events_standardrewards_programenabled_csv',
    'events_standardrewards_programterminated_csv',
    'events_standardrewards_providerjoined_csv',
    'events_standardrewards_providerleft_csv',
    'events_standardrewards_rewardsclaimed_csv',
    'events_standardrewards_rewardsstaked_csv',
    'events_poolcollection_tradingliquidityupdated_spotrates_csv',
    'events_bancornetwork_tokenstraded_updated_csv',
]


# COMMAND ----------

data_dictionary = get_data_dictionary()
data_dictionary.to_csv('/dbfs/FileStore/tables/onchain_events/data_dictionary.csv')
data_dictionary

# COMMAND ----------

ALL_COLUMNS = list(data_dictionary['Column'].values)
NUM_UNIQUE_COLUMNS = len(ALL_COLUMNS)
GOOGLE_SHEETS_MAX_ROWS = int(round(GOOGLE_SHEETS_MAX_CELLS / NUM_UNIQUE_COLUMNS, 0))

for col in ALL_COLUMNS:
    col_type = data_dictionary[data_dictionary['Column'] == col]['Type'].values[0]
    DEFAULT_VALUE_MAP[col] = TYPE_MAP[col_type]['fillna']

# COMMAND ----------

from pyspark.sql.functions import col

list_of_spark_tables = []

# Loops through each table.
for table_name in LIST_OF_SPARK_TABLES:

    #ensure that table exists
    if (spark.sql("show tables in default"
                 ).filter(col("tableName") == f"{table_name}").count() > 0):
        list_of_spark_tables.append(table_name)
    else:
        print(f'table not found {table_name}')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Combine Tables

# COMMAND ----------

unique_col_mapping, combined_df = get_event_mapping(
    spark,
    all_columns=ALL_COLUMNS,
    default_value_map=DEFAULT_VALUE_MAP,
    list_of_spark_tables=list_of_spark_tables
)


# COMMAND ----------


# Loops through each table.
for table_name in list_of_spark_tables:

    # Cleans the google sheets name for clarity.
    clean_table_name = clean_google_sheets_name(table_name)

    # Loads spark tables and converts to pandas
    pdf = get_pandas_df(spark, table_name)

    # Adds a new column with the event name based on table name
    pdf = add_event_name_column(pdf, clean_table_name)

    # Normalizes unique columns across all tables
    pdf = add_missing_columns(pdf, unique_col_mapping, ALL_COLUMNS)

    # Combine the dataframes
    combined_df = concat_dataframes(pdf, combined_df)




# COMMAND ----------

# MAGIC %md
# MAGIC ## Handle Types & Missing Values

# COMMAND ----------

# fills in any remaining missing values for encoder
combined_df = handle_types_and_missing_values(combined_df,
                                              DEFAULT_VALUE_MAP,
                                              ALL_COLUMNS,
                                              TYPE_MAP,
                                              data_dictionary
                                             )
combined_df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging

# COMMAND ----------

# import mlflow

# mlflow.set_experiment("update_google_sheets")

# # Log to mlflow for easy download reference
# combined_df.to_csv('/dbfs/FileStore/tables/combined_df.csv', index=False)
# mlflow.log_artifact('/dbfs/FileStore/tables/combined_df.csv')

# COMMAND ----------

# perform encoding if desired
if len(ADDRESS_COLUMNS) > 0:
    combined_df = encode_address_columns(combined_df, ADDRESS_COLUMNS)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Split Dataframe into chunks

# COMMAND ----------

combined_df = remove_unused_events(combined_df)

# COMMAND ----------

file_size_compatible = False

while not file_size_compatible:
    
    # Splits the pandas dataframe into chunks which conform to the max google sheet size.
    pdf_chunks = split_dataframe(combined_df, TABLEAU_MANAGEABLE_ROWS)
    
    # Store the number of chunks to upload to google sheets
    num_chunks = len(pdf_chunks)
    
    # Recheck if the file size is <= 10MB per tableau requirements
    file_size_compatible = is_file_size_compatible(pdf_chunks)
    
    # Increment size downward by 1000 and try again if not compatible
    if not file_size_compatible:
        TABLEAU_MANAGEABLE_ROWS -= 1000


# COMMAND ----------

# print expected data size for easy reference
num_chunks, len(combined_df), list(combined_df.columns), TABLEAU_MANAGEABLE_ROWS, GOOGLE_SHEETS_MAX_ROWS

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Google Sheets

# COMMAND ----------

for i in range(num_chunks):
    handle_google_sheets(f'{EVENTS_TABLE}_{i}', f'{EVENTS_TABLE}_{i}', pdf_chunks[i])

# COMMAND ----------

delete_unused_google_sheets(num_chunks)

# COMMAND ----------

from collections import Counter
Counter(combined_df.event_name)

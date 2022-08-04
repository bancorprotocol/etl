# Databricks notebook source
import pandas as pd
import glob
from bancor_etl.constants import *

# COMMAND ----------

eventsfiles = glob.glob(ETL_CSV_STORAGE_DIRECTORY+'Events_**')
eventsfiles = [x for x in eventsfiles if 'parquet' in x]

new_events_files = []
for file in eventsfiles:
    df = pd.read_parquet(file)
    if len(df) > 0:
        new_events_files.append(file)
    else:
        print(f'Zero columns for: {file}')
        
print(len(new_events_files))

# COMMAND ----------

# DBTITLE 1,Spark Tables for all "Events"
perm_names = []
for file in new_events_files:
    fileloc = file.replace('/dbfs','')
    perm_name = fileloc.replace('.','_').split('/')[-1].lower()
    perm_names += [perm_name]
#     print(perm_name)

    # File location and type
    file_location = fileloc
    file_type = "parquet"

    # CSV options
    infer_schema = "true"
    first_row_is_header = "true"
    delimiter = ","

    # The applied options are for CSV files. For other file types, these will be ignored.
    df = spark.read.format(file_type) \
      .option("inferSchema", infer_schema) \
      .option("header", first_row_is_header) \
      .option("sep", delimiter) \
      .load(file_location)

    permanent_table_name = perm_name
    df.write.format("parquet").mode("overwrite").saveAsTable(permanent_table_name)

# COMMAND ----------

# Lists events tables that were converted to sparks
perm_names

# COMMAND ----------

# Lists all tables labelled as events
sparktables = [i.name for i in spark.catalog.listTables() if 'events_' in i.name]
print(len(sparktables))
sparktables

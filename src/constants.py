""" 
This file brings configuration constants for the whole project.
"""

# Schematics
CATALOG ="ifood"
LAKE = "lake"
LANDING_ZONE = "landing_zone"

# LAYERS
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"
BRONZE_TABLE = f"{CATALOG}.{BRONZE_SCHEMA}.BRZ_TXI_YEL_TRIP_RAW"
SILVER_TABLE = f"{CATALOG}.{SILVER_SCHEMA}.SIL_TXI_YEL_TRIP"
GOLD_TABLE_MONTHLY= f"{CATALOG}.{GOLD_SCHEMA}.GLD_TXI_YEL_TRIP_MONTHLY"
GOLD_TABLE_WEEKLY= f"{CATALOG}.{GOLD_SCHEMA}.GLD_TXI_YEL_TRIP_WEEKLY"
GOLD_TABLE_DAILY= f"{CATALOG}.{GOLD_SCHEMA}.GLD_TXI_YEL_TRIP_DAILY"
GOLD_TABLE_HOURLY= f"{CATALOG}.{GOLD_SCHEMA}.GLD_TXI_YEL_TRIP_HOURLY"
GOLD_TABLE_MAY_ANALYTICS = f"{CATALOG}.{GOLD_SCHEMA}.GLD_TXI_YEL_TRIP_MAY_ANALYTICS"

# PIPELINE CONFIGURATION - DOWNLOAD TO LANDING ZONE
TARGET_YEAR = "2023"
TARGET_MONTHS_NUMERIC = ["01", "02", "03", "04", "05", "06"]
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
VOLUME_PATH= "/Volumes/ifood/lake/landing_zone"

# PIPELINE CONFIGURATION = LANDING TO BRONZE
LANDING_ZONE_FORMAT = "parquet"
LANDING_ZONE_FILES = f"*.{LANDING_ZONE_FORMAT}"
LANDING_ZONE_DIRECTORY = f"/Volumes/{CATALOG}/{LAKE}/{LANDING_ZONE}/"
LANDING_ZONE_PATH = LANDING_ZONE_DIRECTORY + LANDING_ZONE_FILES

TARGET_RAW_COLUMNS = [
    "VendorID",
    "passenger_count",
    "total_amount",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

METADATA_TYPE_COLUMNS = [
    "FIELD_NAME",
    "VARIABLE_TYPE"
]

AUDIT_COLUMNS = {
    "AUD_ING_SYSTEM" : "Databricks_PySpark_Pipeline",
    #"AUD_ING_SCRIPT" : dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/')[-1]
}
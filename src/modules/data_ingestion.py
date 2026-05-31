# src/modules/data_ingestion.py
from typing import Dict, List, Any
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import pyspark.sql.types as T


def create_metadata_dict(df_dict: Dict[str, "DataFrame"], metadata_type_columns: List[str], verbose:bool=True) -> Dict[str, "DataFrame"]:
    """
    Creates a dictionary of DataFrames with file names as keys and DataFrames as values. The DataFrames contain the metadata for each file as columns VARIABLE_TYPE and FIELD_NAME.

    Parameters:
    df: DataFrame
    A DataFrame containing the metadata for each file as columns VARIABLE_TYPE and FIELD_NAME.

    Returns:
    metadata_dict: Dictionary of Dataframes
    A dictionary with file names as keys and DataFrames as values. The DataFrames contain the metadata for each file as columns VARIABLE_TYPE and FIELD_NAME.
    """

    spark = SparkSession.getActiveSession()
    if not spark:
        raise Exception("Spark session not found.")
    
    # Create metadata dictionaries:
    df_meta_dict = {}
    if verbose:
        print("Creating Metadata Dataframe:")
    for file_name, df in df_dict.items():
        df_meta_dict[file_name] = spark.createDataFrame(
            [(field.name, str(field.dataType)) for field in df.schema],
            metadata_type_columns
        )
        if verbose:
            print(f"\tCreated metadata for {file_name} with {len(df_meta_dict[file_name].columns)} columns {metadata_type_columns}")

    return df_meta_dict


def generate_schema_matrix(metadata_dict: Dict[str, "DataFrame"], files_list: List, verbose:bool=True) -> "DataFrame":
    """
    Consolidates metadata for each file in the landing zone and creates a matrix of schema types for each field for auditing purposes

    Parameters:
    metadata_dict: Dictionary of Dataframes
    A dictionary with file names as keys and DataFrames as values. The DataFrames should contain the metadata for each file as columns VARIABLE_TYPE and FIELD_NAME")
    
    Returns:
    schema_matrix: DataFrame
    A pivoted DataFrame with the schema types for each field for each file in the landing zone. The columns are the file names and the rows are the field names. The values are the schema types for each field for each file in the landing zone.
    
    """

    spark = SparkSession.getActiveSession()
    if not spark:
        raise Exception("Spark session not found.")
    
    if verbose:
        print ("Preparing Schema Matrix")

    
    df_stacked = metadata_dict[files_list[0]].withColumn("File_Name", F.lit(files_list[0]))
    for file_name in files_list[1:]:
        df_stacked = df_stacked \
            .union(metadata_dict[file_name] \
            .withColumn("File_Name", F.lit(file_name))
            )    
        # print("\tFile {file_name} done")

    
    df_schema_matrix = df_stacked \
        .groupBy("File_Name") \
        .pivot("FIELD_NAME") \
        .agg(F.first("VARIABLE_TYPE"))
    
    if verbose:
         print("\tDone")

    return df_schema_matrix


def ingest_data(df_dict: Dict[str, "DataFrame"], table:str, audit_data:Dict[str, Any], overwrite_table:bool = True, overwrite_schema:bool = True, verbose:bool=True) -> bool:
    """
    Ingests data from a dictionary of DataFrames into table.

    Parameters:
    df_dict: Dictionary of DataFrames
    A dictionary with file names as keys and DataFrames as values. The DataFrames, BRONZE_TABLE)
    
    table: str
    The name of the table to ingest the data into.

    Returns:
    bool
    True if the data was successfully ingested, False otherwise.
    """

    spark = SparkSession.getActiveSession()
    if not spark:
        raise Exception("Spark session not found.")


    if overwrite_table:
        query = f"DROP TABLE IF EXISTS {table}"
        spark.sql(query)
        print(f"\t\tTable {table} dropped")   

    # overwrite trigger
    first_df = True 

    if verbose:
        print(f"\t\tIngesting data into table {table}")
    try:
        for file_name, df in df_dict.items():
            df = df \
                .withColumn("AUD_ING_DTTM", F.current_timestamp()) \
                .withColumn("AUD_ING_SYSTEM", F.lit(audit_data["AUD_ING_SYSTEM"])) \
                .withColumn("AUD_ING_SCRIPT", F.lit(audit_data["CURRENT_SCRIPT"])) \
                .withColumn("FILE_NAME", F.lit(file_name))
            
            if first_df:                 
                df.write \
                    .mode("overwrite") \
                    .option("overwriteSchema", overwrite_schema) \
                    .saveAsTable(table)
                first_df = False
            else:
                df.write.mode("append") \
                    .option("mergeSchema", True) \
                    .saveAsTable(table)
            print(f"\t\t\tData ingested: {file_name}")
        return True
    except Exception as e:
        print(f"\nError: {e}")
        return False

def build_gold_agg(source_table: str, 
                   target_table: str,
                   time_slice: str, 
                   verbose: bool=True) -> DataFrame:
    """
    Builds the gold table aggregations.
    
    Parameters
    ----------
    df_silver : DataFrame
        The silver table.
    target_table : str
        The target table.
    time_slice : str
        The time slice.
    verbose : bool, optional
        Whether to print the results, by default True

    """
    
    valid_slices = ['month', 'day', 'hour']
    if time_slice not in valid_slices:
        raise ValueError(f"Invalid time slice: {time_slice}. Valid slices are: {valid_slices}")

    df_silver = spark.table(source_table)
    
    df_aggregated = df_silver \
        .withColumn("TIME_SLICE", F.expr(f"{time_slice}(tpep_pickup_datetime)")) \
        .groupBy("TIME_SLICE") \
        .agg(
            # DIMENSION 1: OPERATIONAL & VOLUMETRICS
            F.count("*").alias("total_trips"),
            F.sum("passenger_count").alias("total_passenger_count"),
            F.avg("passenger_count").alias("avg_passenger_count"),

            # DIMENSION 2: FINANCIAL METRICS (MAIN REVENUE)
            # Total Amount (Final Revenue)
            F.sum("total_amount").alias("total_amount"),
            F.avg("total_amount").alias("avg_total_amount"),
            F.max("total_amount").alias("max_total_amount"),
            F.min("total_amount").alias("min_total_amount"),
            
            # Fare Amount (Base Fare)
            F.sum("fare_amount").alias("total_fare_amount"),
            F.avg("fare_amount").alias("avg_fare_amount"),
            F.max("fare_amount").alias("max_fare_amount"),
            F.min("fare_amount").alias("min_fare_amount"),
            
            # Tip Amount (Driver Tips)
            F.sum("tip_amount").alias("total_tip_amount"),
            F.avg("tip_amount").alias("avg_tip_amount"),
            F.max("tip_amount").alias("max_tip_amount"),
            F.min("tip_amount").alias("min_tip_amount"),

            # DIMENSION 3: SURCHARGES & ADDITIONAL FEES
            # Airport Fee
            F.sum("airport_fee").alias("total_airport_fee"),
            F.avg("airport_fee").alias("avg_airport_fee"),
            F.max("airport_fee").alias("max_airport_fee"),
            F.min("airport_fee").alias("min_airport_fee"),
            
            # Congestion Surcharge
            F.sum("congestion_surcharge").alias("total_congestion_surcharge"),
            F.avg("congestion_surcharge").alias("avg_congestion_surcharge"),
            F.max("congestion_surcharge").alias("max_congestion_surcharge"),
            F.min("congestion_surcharge").alias("min_congestion_surcharge"),
            
            # Improvement Surcharge
            F.sum("improvement_surcharge").alias("total_improvement_surcharge"),
            F.avg("improvement_surcharge").alias("avg_improvement_surcharge"),
            F.max("improvement_surcharge").alias("max_improvement_surcharge"),
            F.min("improvement_surcharge").alias("min_improvement_surcharge"),

            # DIMENSION 4: TRIP METRICS (DISTANCE & TIME)
            # Trip Distance
            F.sum("trip_distance").alias("total_trip_distance"),
            F.avg("trip_distance").alias("avg_trip_distance"),
            F.max("trip_distance").alias("max_trip_distance"),
            F.min("trip_distance").alias("min_trip_distance"),
            
            # Travel Time (Corrected to Seconds to avoid datediff zeroed results)
            F.sum(F.expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")).alias("total_travel_time_seconds"),
            F.avg(F.expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")).alias("avg_travel_time_seconds"),
            F.max(F.expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")).alias("max_travel_time_seconds"),
            F.min(F.expr("unix_timestamp(tpep_dropoff_datetime) - unix_timestamp(tpep_pickup_datetime)")).alias("min_travel_time_seconds")
        )
        
    print(f"Aggregated by {time_slice}")
    return df_aggregated  
    
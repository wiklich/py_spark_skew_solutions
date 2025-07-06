"""
Spark Utility Module

This module contains general purpose utilities for working with PySpark, including:
- Spark session management
- DataFrame manipulation
- Logging integration
"""

import logging
from typing import Dict, Optional
from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame


# Configure logging
logger = logging.getLogger(__name__)

def create_or_get_spark_session(app_name: str, configs: Optional[Dict[str, str]] = None) -> SparkSession:
    """
    Creates or retrieves an existing SparkSession with the given application name and optional configurations.

    Args:
        app_name (str): Name of the Spark application.
        configs (Optional[Dict[str, str]]): Dictionary of Spark configuration key-value pairs.

    Returns:
        SparkSession: An active SparkSession instance.
    """
    logger.info("Creating or retrieving SparkSession with app name: %s", app_name)

    builder = SparkSession.builder.appName(app_name)

    if configs:
        for key, value in configs.items():
            logger.debug("Setting Spark config: %s = %s", key, value)
            builder = builder.config(key, value)

    spark = builder.getOrCreate()
    logger.debug("SparkSession created with ID: %s", spark.sparkContext.applicationId)

    return spark


def get_active_spark_session() -> Optional[SparkSession]:
    """
    Retrieves the currently active SparkSession, if any.

    Returns:
        Optional[SparkSession]: The active SparkSession or None if not found.
    """
    logger.info("Attempting to retrieve active SparkSession")
    active_session = SparkSession.getActiveSession()

    if active_session is not None:
        logger.info("Active SparkSession found with ID: %s", active_session.sparkContext.applicationId)
    else:
        logger.warning("No active SparkSession found")

    return active_session


def require_active_spark_session() -> SparkSession:
    """
    Ensures that there's an active SparkSession or raises an error.
    """
    session = get_active_spark_session()
    if session is None:
        logger.error("No active SparkSession found")
        raise RuntimeError("Expected an active SparkSession but none was found.")
    return session


def stop_spark_session(spark: SparkSession) -> None:
    """
    Stops the given SparkSession gracefully.

    Args:
        spark (SparkSession): The SparkSession to stop.
    """
    if spark.sparkContext._jsc is not None:
        app_id = spark.sparkContext.applicationId
        logger.info("Stopping SparkSession with ID: %s", app_id)
        spark.stop()
        logger.info("SparkSession with ID: %s stopped successfully", app_id)
    else:
        logger.warning("SparkSession is already stopped")


def save_dataframes_to_parquet(dataframes_dict: Dict[str, DataFrame]) -> None:
    """
    Saves multiple DataFrames to Parquet format using provided paths as keys.

    Args:
        dataframes_dict (Dict[str, DataFrame]): A dictionary where keys are full output paths
                                            and values are the corresponding DataFrames to save.
    """
    for output_path, df in dataframes_dict.items():
        logger.info("Saving DataFrame to Parquet path: %s", output_path)
        df.write \
            .mode("overwrite") \
            .parquet(output_path)
        logger.info("DataFrame successfully saved to Parquet at: %s", output_path)


def save_dataframe_to_parquet(df: DataFrame, output_full_path: str) -> None:
    """
    Saves a single DataFrame to Parquet format at the specified path by using
    the `save_dataframes_to_parquet` function.

    Args:
        df (DataFrame): The DataFrame to save.
        output_full_path (str): The full path where the DataFrame will be saved as Parquet.
    """
    save_dataframes_to_parquet({output_full_path: df})


def read_parquet(input_path: str, spark: Optional[SparkSession] = None) -> DataFrame:
    """
    Reads a Parquet file into a Spark DataFrame with logging.

    Args:
        input_path (str): Path to the Parquet dataset.
        spark (Optional[SparkSession]): An active SparkSession. If not provided,
                                         an active session will be retrieved or created.

    Returns:
        DataFrame: The loaded Spark DataFrame.

    Raises:
        RuntimeError: If no SparkSession is available and one cannot be created.
        AnalysisException: If the path does not contain valid Parquet files.
    """
    if spark is None:
        logger.info("No SparkSession provided, attempting to retrieve an active one")
        spark = get_active_spark_session()
        if spark is None:
            logger.error("❌ No active SparkSession found for reading Parquet file.")
            raise RuntimeError("Cannot read Parquet without an active SparkSession")

    logger.info("📈 Reading Parquet data from: %s", input_path)

    try:
        df = spark.read.parquet(input_path)
        logger.info("✅ Successfully read Parquet data from: %s.", input_path)
        return df
    except Exception as e:
        logger.error("❌ Failed to read Parquet dataset from %s: %s", input_path, e, exc_info=True)
        raise


def create_spark_configs_for_skew_demo(aqe_enabled: bool, shuffle_partitions: int = 200) -> Dict[str, str]:
        """
        Returns a dictionary of Spark configurations based on skew demonstration mode and AQE settings.

        Args:
            demonstrate_skew (bool): If True, returns fixed configs to highlight skew behavior.
            aqe_enabled (bool): If True, enables Adaptive Query Execution and Skewed Join handling.
            shuffle_partitions (int): Number of shuffle partitions when AQE is enabled. Default: 200.

        Returns:
            Dict[str, str]: Dictionary of Spark configuration key-value pairs.
        """
        return {
            "spark.sql.adaptive.enabled": str(aqe_enabled).lower(),
            "spark.sql.adaptive.skewedJoin.enabled": str(aqe_enabled).lower(),
            "spark.sql.shuffle.partitions": str(shuffle_partitions)
        }

def log_spark_config(spark: SparkSession) -> None:
    """
    Logs all current Spark configuration parameters from the active SparkSession.

    Args:
        spark (SparkSession): The active SparkSession to extract configurations from.
    """
    conf_items = spark.sparkContext.getConf().getAll()

    logger.info("📋 Current SparkSession configuration:")
    for key, value in sorted(conf_items):
        logger.info("  %s = %s", key, value)


def get_max_partition_size_in_bytes(conf: SparkConf) -> int:
    """
    Retrieves the maximum partition size in bytes.
    
    Args:
        conf (SparkConf): The Spark configuration object.

    Returns:
        int: Maximum partition size in bytes.
    """
    return int(conf.get("spark.sql.files.maxPartitionBytes", "134217728"))


def get_max_partition_size_in_mb(conf: SparkConf) -> int:
    """
    Retrieves the maximum partition size in bytes and converts it to megabytes.
    
    Args:
        conf (SparkConf): The Spark configuration object.

    Returns:
        int: Maximum partition size in megabytes (rounded down).
    """
    max_partition_bytes = get_max_partition_size_in_bytes(conf=conf)
    max_partition_mb = max_partition_bytes // (1024 * 1024)
    return max_partition_mb


def get_executor_memory_in_gb(conf: SparkConf) -> float:
    """
    Extracts the executor memory from Spark configuration and converts it to gigabytes.
    If not set, defaults to '4g'.

    Args:
        conf (SparkConf): The Spark configuration object.
        logger (logging.Logger): Logger for warnings and messages.

    Returns:
        float: Memory per executor in gigabytes.
    """
    executor_memory_str = conf.get("spark.executor.memory", None)
    if not executor_memory_str:
        executor_memory_str = "4g"
        logger.warning("spark.executor.memory not set. Using default value: 4g")

    # Remove case and unit to extract numeric value
    mem_value = float(executor_memory_str.lower().replace("g", "").replace("m", ""))

    # Convert MB to GB if necessary
    if "m" in executor_memory_str.lower():
        mem_value /= 1024

    return mem_value


def get_cores_per_executor(conf: SparkConf) -> int:
    """
    Retrieves the number of cores per executor from Spark configuration.
    If not set, defaults to 1.

    Args:
        conf (SparkConf): The Spark configuration object.
        logger (logging.Logger): Logger for warnings and messages.

    Returns:
        int: Number of cores per executor.
    """
    cores_per_executor = conf.get("spark.executor.cores", None)
    if not cores_per_executor:
        cores_per_executor = "1"
        logger.warning("spark.executor.cores not set. Using default value: 1")
    return int(cores_per_executor)


def get_executor_instances_number(spark: SparkSession) -> int:
    """
    Retrieves the configured number of executors from the SparkSession.
    
    If `spark.executor.instances` is not set in the configuration,
    it defaults to 1 and logs a warning message.

    Args:
        spark (SparkSession): Active SparkSession instance used to access Spark configurations.

    Returns:
        int: The number of executors configured for the current Spark application.
    """
    conf = spark.sparkContext.getConf()

    try:
        num_executors = int(conf.get("spark.executor.instances"))
        logger.info("Found %d executors from configuration.", num_executors)
    except (TypeError, ValueError):
        logger.warning(
            "spark.executor.instances not set or invalid. Using default value: 1"
        )
        num_executors = 1

    return num_executors
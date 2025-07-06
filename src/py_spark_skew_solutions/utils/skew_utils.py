"""
skew_utils.py

Utility functions to help mitigate data skew in PySpark by estimating optimal bucket sizes for salting or repartitioning.

This module provides a dynamic and robust method to calculate the ideal number of buckets based on:
- Average record size
- Executor memory and core configuration
- Operation type (e.g., shuffle-heavy vs. transform)
- Spark configuration (spark.sql.files.maxPartitionBytes)

All logging, comments, and documentation are in English.
"""

from enum import Enum
from typing import Optional
from pyspark.sql import DataFrame, SparkSession
import logging
import pyspark.sql.functions as F
from py_spark_skew_solutions.utils.spark_utils import (
    get_max_partition_size_in_mb,
    get_executor_memory_in_gb,
    get_cores_per_executor
)


logger = logging.getLogger(__name__)

SALT_PREFIX = "__SALT__"

class OperationType(str, Enum):
    """
    Enumeration of operation types used to determine optimal partition size.

    Each type maps to a recommended target partition size in MB.
    """

    SHUFFLE_LIGHT = "shuffle_light"
    SHUFFLE_HEAVY = "shuffle_heavy"
    AGGREGATE = "aggregate"
    TRANSFORM = "transform"
    DEFAULT = "default"

    @classmethod
    def get_default_partition_size(cls, operation_type: "OperationType") -> int:
        """
        Returns the default target partition size (in MB) based on operation type.

        Args:
            operation_type (OperationType): Type of operation.

        Returns:
            int: Target partition size in MB.
        """
        size_map = {
            cls.SHUFFLE_LIGHT: 256,
            cls.SHUFFLE_HEAVY: 128,
            cls.AGGREGATE: 128,
            cls.TRANSFORM: 512,
            cls.DEFAULT: 256,
        }
        return size_map.get(operation_type, size_map[cls.DEFAULT])


def estimate_avg_record_size(df: DataFrame, sample_fraction: float = 0.01) -> float:
    """
    Estimates the average record size in bytes using a small sample of the DataFrame.
    
    This is useful to avoid full scans when calculating partition sizes for salting or repartitioning.
    
    Args:
        df (DataFrame): Input DataFrame.
        sample_fraction (float): Fraction of the data to sample (e.g., 0.01 = 1%). Default: 0.01.

    Returns:
        float: Estimated average record size in bytes.
    """
    logger.info(f"📊 Estimating average record size using {sample_fraction * 100:.2f}% sample")

    try:
        # Sample the DataFrame
        sample_df = df.sample(withReplacement=False, fraction=sample_fraction)

        # Estimate total size of sampled data
        sample_size_bytes = sample_df.agg(F.sum(F.length(F.concat(*df.columns)))).first()[0]

        # Calculate average record size
        avg_record_size_bytes = sample_size_bytes / sample_df.count()

        logger.info(f"📏 Estimated average record size: {avg_record_size_bytes:.2f} bytes per row")
        return avg_record_size_bytes

    except Exception as e:
        logger.warning("⚠️ Could not compute exact record size, using default value: 200 bytes", exc_info=True)
        return 200.0


def estimate_optimal_buckets(
    df: DataFrame,
    spark: SparkSession,
    operation_type: OperationType = OperationType.SHUFFLE_HEAVY,
    total_rows: int = 0,
    target_partition_size_mb: float = None,
    safety_factor: float = 0.7
) -> int:
    """
    Estimates the optimal number of buckets for operations like salting or repartitioning,
    taking into account executor resources, average record size, and operation type.

    Args:
        df (DataFrame): Input DataFrame.
        spark (SparkSession): Active Spark session.
        operation_type (OperationType): Type of operation ("shuffle_light", "shuffle_heavy", "aggregate", "transform")
        total_rows (int): Total Rows in Dataframe, if not informed it will be used df.count().
        target_partition_size_mb (float): Target partition size in MB. Default is determined by operation type.
        safety_factor (float): Percentage of executor memory used to avoid OOM (default: 0.7 → 70%).

    Returns:
        int: Recommended number of buckets.
    """

    # Ensure valid operation type
    try:
        operation_type = OperationType(operation_type)
    except ValueError:
        logger.warning(f"Invalid operation_type '{operation_type}'. Using default: {OperationType.DEFAULT}")
        operation_type = OperationType.DEFAULT

    logger.info("📊 Starting bucket estimation process")

    conf = spark.sparkContext.getConf()

    # --- Step 1: Determine target partition size based on operation type ---
    if target_partition_size_mb is None:
        target_partition_size_mb = OperationType.get_default_partition_size(operation_type)

    logger.info(f"Using target partition size: {target_partition_size_mb} MB")

    # --- Step 2: Warn if maxPartitionBytes < target_partition_size_mb ---
    try:        
        max_partition_mb = get_max_partition_size_in_mb(conf=conf)

        logger.info(f"spark.sql.files.maxPartitionBytes = {max_partition_mb} MB")

        if max_partition_mb < target_partition_size_mb:
            logger.warning(
                f"spark.sql.files.maxPartitionBytes ({max_partition_mb} MB) "
                f"< target_partition_size_mb ({target_partition_size_mb} MB). "
                "This may cause overhead from many small partitions. "
                "Consider increasing spark.sql.files.maxPartitionBytes or reducing target_partition_size_mb."
            )

    except Exception as e:
        logger.warning(
            f"Could not parse spark.sql.files.maxPartitionBytes value. Skipping comparison. Error: {e}"
        )

    ## If not total_rows parameter was not informed, so it will be used df.count()
    if total_rows <= 0:
        total_rows = df.count()        

    # --- Step 3: Estimate average record size (in bytes) ---
    try:        
        avg_record_size_bytes = estimate_avg_record_size(df)
        logger.info(f"Estimated average record size: {avg_record_size_bytes:.2f} bytes")
    except Exception as e:
        logger.warning("Could not compute exact record size, using default of 200 bytes")
        avg_record_size_bytes = 200  # Safe default

    logger.info(f"Estimated average record size: {avg_record_size_bytes:.2f} bytes")

    # --- Step 4: Consider executor memory to avoid OOM ---
    try:
        # Extract executor memory
        mem_per_executor_gb = get_executor_memory_in_gb(conf=conf)

        # How many tasks per executor?
        cores_per_executor = get_cores_per_executor(conf=conf)

        # Calculate safe partition size based on executor memory
        safe_partition_bytes = int((mem_per_executor_gb * 1024 * 1024 * 1024) * safety_factor / cores_per_executor)

        logger.info(f"Executor memory: {mem_per_executor_gb} GB")
        logger.info(f"Cores per executor: {cores_per_executor}")
        logger.info(f"Safe partition size: {safe_partition_bytes / (1024 * 1024):.2f} MB")

        # Limit target partition size by safe value
        target_partition_size_mb = min(
            target_partition_size_mb or float("inf"),
            safe_partition_bytes / (1024 * 1024),
        )

    except Exception as e:
        logger.warning(f"Could not extract executor memory config: {e}. Using default values.")

    # --- Step 5: Compute records per partition ---
    records_per_partition = max(
        1, int((target_partition_size_mb * 1024 * 1024) / avg_record_size_bytes)
    )
    logger.info(f"Estimated records per partition: {records_per_partition}")

    # --- Step 6: Compute initial number of buckets ---
    total_cores = spark.sparkContext.defaultParallelism
    num_buckets = max(1, df.rdd.getNumPartitions(), int(total_rows / records_per_partition))

    # --- Step 7: Clamp to reasonable limits ---
    max_buckets_by_cores = total_cores * 2  # Extra parallelism headroom
    num_buckets = min(max(num_buckets, 10), max_buckets_by_cores)

    logger.info(f"Final estimated number of buckets: {num_buckets}")
    return num_buckets


def estimate_optimal_coalesce_partitions(
    df: DataFrame,
    spark: SparkSession,
    operation_type: OperationType = OperationType.AGGREGATE,
    target_partition_size_mb: Optional[float] = None,
    safety_factor: float = 0.7,
    total_rows: int = 0,
) -> int:
    """
    Estimate the optimal number of partitions for coalesce based on average record size and executor memory.

    Designed to reduce small partitions in cold key scenarios by adapting partition size to the type of operation.

    Args:
        df (DataFrame): Input DataFrame to estimate for.
        spark (SparkSession): Active Spark session for configuration access.
        operation_type (OperationType): Type of operation being performed. Used to determine default target partition size.
                                          Options: AGGREGATE, SHUFFLE_HEAVY, SHUFFLE_LIGHT, TRANSFORM, DEFAULT.
        target_partition_size_mb (Optional[float]): Target partition size in MB. If not provided, it will be determined
                                                    using the operation type.
        safety_factor (float): Fraction of executor memory to use safely to avoid OOM. Default: 0.7.
        total_rows (int): Total number of rows in the DataFrame. If not provided, it will be computed via `df.count()`.

    Returns:
        int: Estimated optimal number of partitions for coalesce.
    """
    logger.info("📊 Estimating optimal coalesce partitions")

    conf = spark.sparkContext.getConf()

    # Step 1: Estimate average record size
    avg_record_size_bytes = estimate_avg_record_size(df)
    
    # Step 2: Get total number of rows if not provided
    if total_rows <= 0:
        total_rows = df.count()
    logger.info(f"Total rows: {total_rows}, Avg record size: {avg_record_size_bytes:.2f} bytes")

    # Step 3: Determine target partition size based on operation type
    if target_partition_size_mb is None:
        target_partition_size_mb = OperationType.get_default_partition_size(operation_type)
    logger.info(f"Using target partition size: {target_partition_size_mb} MB for operation type '{operation_type}'")

    # Step 4: Calculate how many records fit into one partition of target size
    records_per_partition = max(1, int((target_partition_size_mb * 1024 * 1024) / avg_record_size_bytes))
    estimated_partitions = max(1, int(total_rows / records_per_partition))

    # Step 5: Adjust based on executor memory to avoid OOM
    try:
        mem_per_executor_gb = get_executor_memory_in_gb(conf=conf)
        cores_per_executor = get_cores_per_executor(conf=conf)

        safe_partition_bytes = int(
            (mem_per_executor_gb * 1024 * 1024 * 1024) * safety_factor / cores_per_executor
        )
        max_records_by_memory = max(1, int(safe_partition_bytes / avg_record_size_bytes))
        max_partitions_by_memory = max(1, int(total_rows / max_records_by_memory))

        estimated_partitions = min(estimated_partitions, max_partitions_by_memory)
    except Exception as e:
        logger.warning(
            f"Could not extract executor config: {e}. Proceeding with default estimation."
        )

    logger.info(f"Estimated optimal coalesce partitions: {estimated_partitions}")
    return estimated_partitions
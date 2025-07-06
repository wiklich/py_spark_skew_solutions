"""
Scenario D Demonstration Module
Implements BaseDemonstration for Scenario D: High cardinality with uneven distribution.
Demonstrates how to mitigate hidden skew using dynamic salting when AQE is disabled.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from py_spark_skew_solutions.demonstration.base_demonstration import BaseDemonstration
from py_spark_skew_solutions.utils.spark_utils import (
    create_or_get_spark_session,
    create_spark_configs_for_skew_demo,
    log_spark_config,
    stop_spark_session,
    save_dataframe_to_parquet,
    read_parquet
)
from py_spark_skew_solutions.utils.skew_utils import (
    estimate_optimal_buckets,
    OperationType,
    SALT_PREFIX
)
from py_spark_skew_solutions.utils.common_utils import validate_required_params


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioD1Demonstration(BaseDemonstration):
    """
    Demonstration class for Scenario D1 (hidden data skew).
    This scenario simulates a dataset with high cardinality where some keys are slightly more frequent than others,
    making the skew difficult to detect. Demonstrates mitigation via dynamic salting strategy when AQE is disabled.
    When AQE is enabled, it uses Spark's built-in skewedJoin optimization.
    """

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 200,
        **kwargs
    ) -> None:
        """
        Run the full demonstration workflow for Scenario D.

        If AQE is enabled, no manual intervention is needed — Spark handles skew automatically.
        If AQE is disabled, we apply dynamic salting to spread out the hot keys and reduce shuffle bottleneck.

        Args:
            aqe_enabled (bool): Whether to enable Spark AQE optimizations.
            demonstrate_skew (bool): If True, will not apply any mitigation to highlight skew effects.
            shuffle_partitions (int): Number of shuffle partitions when AQE is disabled. Default: 200.
            **kwargs: Additional parameters for demonstration logic. Must include:
                - input_path (str): Path to read input data from.
                - output_path (str): Path to write results to.
        """
        # Validate required parameters
        validate_required_params(kwargs, required_keys=["input_path", "output_path"])

        # Retrieve parameteres from kwargs
        input_path = kwargs["input_path"]
        output_path = kwargs["output_path"]

        if demonstrate_skew:
            aqe_enabled = False
            shuffle_partitions = 200  # Use default shuffle partition count to exaggerate skew

        logger.info("⚙️ Starting Scenario D demonstration")

        # General configurations for Spark session
        configs = create_spark_configs_for_skew_demo(aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions)
        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)

        # Initialize Spark session
        spark = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark.sparkContext.setLogLevel("WARN")

        # Log Spark session configs
        log_spark_config(spark=spark)

        try:
            logger.info(f"Reading data from {input_path}")
            df = read_parquet(input_path=input_path, spark=spark)

            if demonstrate_skew:
                result_df = self._demonstrate_skew(df)
            else:
                result_df = self._solve_skew(df=df, spark=spark)

            save_dataframe_to_parquet(df=result_df, output_full_path=output_path)
        except Exception as e:
            logger.error(f"❌ Error during demonstration: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(spark=spark)

    def _demonstrate_skew(self, df: DataFrame) -> DataFrame:
        """
        Simulate skew behavior by performing shuffle operation on skewed data.
        Args:
            df (DataFrame): Input DataFrame.
        Returns:
            DataFrame: Aggregated result showing skew impact.
        """
        logger.info("📊 Demonstrating skew behavior in Scenario D...")
        return df.groupBy("id").agg(F.count("*").alias("count"))

    def _solve_skew(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Process data solving hidden skew using dynamic salting strategy.
        This method dynamically detects hot keys based on their percentage of total data,
        applies salting only to those keys, making the approach scalable even when there
        are millions of hot keys.

        Args:
            df (DataFrame): Input DataFrame.
            spark (SparkSession): The SparkSession to estimate resources from.

        Returns:
            DataFrame: Final aggregated result with skew mitigated.
        """
        logger.info("📦 Solving hidden skew with dynamic salting strategy...")

        total_rows = df.count()

        # --- Step 1: Estimate optimal number of salt buckets ---
        num_buckets = estimate_optimal_buckets(
            df=df,
            spark=spark,
            operation_type=OperationType.AGGREGATE
        )
        logger.info(f"Using {num_buckets} salt buckets for mitigation")

        # --- Step 2: Compute key distribution and persist it ---
        key_distribution = (
            df.groupBy("id")
            .count()
            .withColumn("percentage", F.col("count") / F.lit(total_rows))
            .filter(F.col("percentage") > 0.05)  # Threshold can be adjusted
            .select("id")
        ).cache()
        key_distribution.createOrReplaceTempView("hot_keys")

        # --- Step 3: Split input DF into hot and cold partitions ---
        # Hot partition: rows where id is in hot_keys
        df_hot = df.join(
            spark.table("hot_keys").withColumnRenamed("id", "hot_id"),
            on=F.col("id") == F.col("hot_id"),
            how="inner"
        ).drop("hot_id")

        # Cold partition: all other rows
        df_cold = df.join(
            spark.table("hot_keys").withColumnRenamed("id", "hot_id"),
            on=F.col("id") == F.col("hot_id"),
            how="left_anti"
        )

        logger.info(f"Split completed: {df_hot.count()} hot records, {df_cold.count()} cold records")

        # --- Step 4: Apply salting only to hot partition ---
        df_hot_salted = df_hot.withColumn(
            "salt",
            F.monotonically_increasing_id() % num_buckets
        ).withColumn(
            "salted_id",
            F.concat(F.col("id"), F.lit(SALT_PREFIX), F.col("salt").cast("string"))
        ).select(
            F.col("salted_id").alias("id"),
            F.col("value")
        )

        # --- Step 5: Repartition both datasets before aggregation ---
        df_hot_salted = df_hot_salted.repartition(num_buckets, "id")
        df_cold = df_cold.repartition("id")

        # --- Step 6: Aggregate each dataset separately ---
        df_hot_agg = df_hot_salted.groupBy("id").agg(F.count("*").alias("partial_count"))
        df_cold_agg = df_cold.groupBy("id").agg(F.count("*").alias("partial_count"))

        # --- Step 7: Combine results safely ---
        df_combined = df_hot_agg.unionByName(df_cold_agg, allowMissingColumns=True)

        # --- Step 8: Remove salt and aggregate final counts ---
        df_final = (
            df_combined
            .withColumn("original_id", F.split(F.col("id"), SALT_PREFIX).getItem(0))
            .drop("id")
            .groupBy("original_id")
            .agg(F.sum("partial_count").alias("total_count"))
        )

        return df_final
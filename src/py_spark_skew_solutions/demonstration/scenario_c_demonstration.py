"""
Scenario C Demonstration Module
Implements BaseDemonstration for Scenario C: Mix of many small keys and few dominant ones.
Demonstrates how to mitigate mixed data skew using conditional salting and coalesce optimization when AQE is disabled.
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
    read_parquet,
)
from py_spark_skew_solutions.utils.skew_utils import (
    estimate_optimal_buckets,
    OperationType,
    estimate_optimal_coalesce_partitions,
    SALT_PREFIX
)
from py_spark_skew_solutions.utils.common_utils import validate_required_params


logger = logging.getLogger(__name__)


class ScenarioCDemonstration(BaseDemonstration):
    """
    Demonstration class for Scenario C (mixed data skew).
    This scenario simulates a dataset with a mix of many small keys and a few hot keys,
    and demonstrates mitigation via conditional salting + coalesce strategy when AQE is disabled.

    When AQE is enabled, it uses Spark's built-in skewedJoin optimization.
    """

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 50,
        **kwargs
    ) -> None:
        """
        Run the full demonstration workflow for Scenario C.

        If AQE is enabled, no manual intervention is needed — Spark handles skew automatically.
        If AQE is disabled, we apply conditional salting only on the hot keys and coalesce on cold keys.

        Args:
            aqe_enabled (bool): Whether to enable Spark AQE optimizations.
            demonstrate_skew (bool): If True, will not apply any mitigation to highlight skew effects.
            shuffle_partitions (int): Number of shuffle partitions when AQE is disabled. Default: 50.
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
            shuffle_partitions = 200

        logger.info("⚙️ Starting Scenario C demonstration")

        configs = create_spark_configs_for_skew_demo(
            aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions
        )
        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)

        spark = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark.sparkContext.setLogLevel("WARN")

        log_spark_config(spark=spark)

        try:
            logger.info(f"Reading data from {input_path}")
            df = read_parquet(input_path=input_path, spark=spark)

            if demonstrate_skew:
                result_df = self._demonstrate_skew(df=df)
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
        Demonstrates skew behavior by performing an aggregation on skewed data.
        """
        logger.info("📊 Demonstrating skew behavior in Scenario C...")
        return df.groupBy("id").agg(F.count("*").alias("count"))

    def _solve_skew(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Mitigates mixed skew by dynamically identifying hot keys and applying salting only to those.
        Cold keys are coalesced to reduce task scheduling overhead.

        Args:
            df (DataFrame): Input DataFrame.
            spark (SparkSession): The SparkSession to estimate resources from.

        Returns:
            DataFrame: Aggregated and skew-mitigated DataFrame.
        """
        logger.info("📦 Solving Skew in Scenario C with conditional salting and coalesce...")

        total_rows = df.count()

        num_buckets = estimate_optimal_buckets(
            df=df,
            spark=spark,
            operation_type=OperationType.AGGREGATE,
            total_rows=total_rows
        )
        logger.info(f"Using {num_buckets} salt buckets for mitigation")

        key_distribution = (
            df.groupBy("id")
            .count()
            .withColumn("percentage", F.col("count") / F.lit(total_rows))
            .filter(F.col("percentage") > 0.05)
            .select("id")
        )

        key_distribution.cache()
        hot_keys = [row["id"] for row in key_distribution.collect()]
        logger.info(f"Detected {len(hot_keys)} hot keys: {hot_keys[:5]}...")

        df_hot = df.filter(F.col("id").isin(hot_keys))
        df_cold = df.filter(~F.col("id").isin(hot_keys))

        logger.info(f"Split completed: {df_hot.count()} hot records, {df_cold.count()} cold records")

        # --- Apply salting to hot keys ---
        df_hot_salted = df_hot.withColumn(
            "salt",
            F.monotonically_increasing_id() % num_buckets
        ).withColumn(
            "salted_id",
            F.concat(F.col("id"), F.lit(SALT_PREFIX), F.col("salt").cast("string"))
        )

        df_hot_salted = df_hot_salted.select(
            F.col("salted_id").alias("id"),
            F.col("value")
        )

        # --- Coalesce cold keys to reduce small partitions ---
        cold_partitions = estimate_optimal_coalesce_partitions(
            df=df_cold, 
            spark=spark,
            total_rows=total_rows
        )
        logger.info(f"Coalescing cold keys to {cold_partitions} partitions")
        df_cold = df_cold.coalesce(cold_partitions)

        # --- Repartition both datasets before aggregation ---
        df_hot_salted = df_hot_salted.repartition(num_buckets, "id")
        df_cold = df_cold.repartition("id")

        # --- Aggregate separately ---
        df_hot_agg = df_hot_salted.groupBy("id").agg(F.count("*").alias("partial_count"))
        df_cold_agg = df_cold.groupBy("id").agg(F.count("*").alias("partial_count"))

        # Ensure compatible schema
        df_hot_agg = df_hot_agg.select("id", "partial_count")
        df_cold_agg = df_cold_agg.select(
            F.col("id").cast("string").alias("id"),
            F.col("partial_count").cast("long").alias("partial_count")
        )

        # Combine results
        df_combined = df_hot_agg.union(df_cold_agg)

        # Remove salt and finalize
        df_final = (
            df_combined
            .withColumn("original_id", F.split(F.col("id"), SALT_PREFIX).getItem(0))
            .drop("id")
            .groupBy("original_id")
            .agg(F.sum("partial_count").alias("total_count"))
        )

        return df_final
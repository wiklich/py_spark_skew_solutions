"""
Scenario D3 Demonstration Module
Implements BaseDemonstration for Scenario D3: Temporal skew with high volume in certain time periods.
Demonstrates how to mitigate seasonal skew using dynamic salting when AQE is disabled.
"""

import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType

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


class ScenarioD3Demonstration(BaseDemonstration):
    """
    Demonstration class for Scenario D3 (temporal skew).
    This scenario simulates a dataset where certain time periods have disproportionately more records,
    such as logs during peak hours. Demonstrates mitigation via dynamic salting strategy when Adaptive Query Execution (AQE) is disabled.
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
        Run the full demonstration workflow for Scenario D3.

        If AQE is enabled, no manual intervention is needed — Spark handles skew automatically.
        If AQE is disabled, we apply dynamic salting to spread out hot time periods and reduce shuffle bottleneck.

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
            shuffle_partitions = 200  # Use default to exaggerate skew

        logger.info("⚙️ Starting Scenario D3 demonstration")

        configs = create_spark_configs_for_skew_demo(aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions)
        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)

        spark = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark.sparkContext.setLogLevel("WARN")

        log_spark_config(spark=spark)

        try:
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
        Simulate skew behavior by performing shuffle operation on skewed temporal data.

        Args:
            df (DataFrame): Input DataFrame.

        Returns:
            DataFrame: Aggregated result showing skew impact.
        """
        logger.info("📊 Demonstrating skew behavior in Scenario D3...")

        return df.withColumn("hour", F.hour(F.col("ts"))) \
                 .groupBy("hour") \
                 .agg(F.count("*").alias("total_count"))

    def _solve_skew(self, df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Process data solving temporal skew using dynamic salting strategy.
        This method dynamically detects hot time windows (based on hour),
        applies salting only to those timestamps, making the approach scalable even when there
        are millions of records concentrated in one hour.

        Args:
            df (DataFrame): Input DataFrame.
            spark (SparkSession): The SparkSession to estimate resources from.

        Returns:
            DataFrame: Final aggregated result with skew mitigated.
        """
        logger.info("📦 Solving temporal skew with dynamic salting strategy...")

        total_rows = df.count()

        # --- Step 1: Extract hour from timestamp for analysis ---
        df_with_hour = df.withColumn("hour", F.hour(F.col("ts")))

        # --- Step 2: Estimate optimal number of salt buckets ---
        num_buckets = estimate_optimal_buckets(
            df=df_with_hour,
            spark=spark,
            operation_type=OperationType.AGGREGATE
        )
        logger.info(f"Using {num_buckets} salt buckets for mitigation")

        # --- Step 3: Compute hourly distribution and persist it ---
        key_distribution = (
            df_with_hour.groupBy("hour")
            .count()
            .withColumn("percentage", F.col("count") / F.lit(total_rows))
            .filter(F.col("percentage") > 0.05)  # Threshold can be adjusted
            .select("hour")
        ).cache()
        key_distribution.createOrReplaceTempView("hot_hours")

        # --- Step 4: Split input DF into hot and cold partitions ---
        # Hot partition: rows where hour is in hot_hours
        df_hot = df_with_hour.join(
            spark.table("hot_hours").withColumnRenamed("hour", "hot_hour"),
            on=F.col("hour") == F.col("hot_hour"),
            how="inner"
        ).drop("hot_hour")

        # Cold partition: all other rows
        df_cold = df_with_hour.join(
            spark.table("hot_hours").withColumnRenamed("hour", "hot_hour"),
            on=F.col("hour") == F.col("hot_hour"),
            how="left_anti"
        )

        logger.info(f"Split completed: {df_hot.count()} hot records, {df_cold.count()} cold records")

        # --- Step 5: Apply salting only to hot partition ---
        df_hot_salted = df_hot.withColumn(
            "salt",
            F.monotonically_increasing_id() % num_buckets
        ).withColumn(
            "salted_ts",
            F.concat(
                F.col("ts").cast("string"), F.lit(SALT_PREFIX), F.col("salt").cast("string")
            ).cast(TimestampType())
        ).select(
            F.col("salted_ts").alias("ts"),
            F.col("id")
        )

        # --- Step 6: Repartition both datasets before aggregation ---
        df_hot_salted = df_hot_salted.repartition(num_buckets, "ts")
        df_cold = df_cold.repartition("ts")

        # --- Step 7: Aggregate each dataset separately ---
        df_hot_agg = df_hot_salted.withColumn("hour", F.hour(F.col("ts"))) \
                                  .groupBy("hour") \
                                  .agg(F.count("*").alias("partial_count"))
        df_cold_agg = df_cold.withColumn("hour", F.hour(F.col("ts"))) \
                             .groupBy("hour") \
                             .agg(F.count("*").alias("partial_count"))

        # --- Step 8: Combine results safely ---
        df_combined = df_hot_agg.unionByName(df_cold_agg, allowMissingColumns=True)

        # --- Step 9: Final aggregation to sum partial counts ---
        df_final = df_combined.groupBy("hour").agg(
            F.sum("partial_count").alias("total_count")
        )

        return df_final
"""
Scenario B Demonstration Module

Implements BaseDemonstration for Scenario B: Few Keys with Many Records.
Demonstrates how to mitigate skew using salting when AQE is disabled.
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


class ScenarioBDemonstration(BaseDemonstration):
    """
    Demonstration class for Scenario B (classic data skew).

    This scenario simulates a dataset where one key dominates the distribution,
    and demonstrates mitigation via salting strategy when Adaptive Query Execution (AQE) is disabled.

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
        Run the full demonstration workflow for Scenario B.

        If AQE is enabled, no manual intervention is needed — Spark handles skew automatically.
        If AQE is disabled, we apply salting to spread out the dominant key and reduce shuffle bottleneck.

        Args:
            aqe_enabled (bool): Whether to enable Spark AQE optimizations.
            demonstrate_skew (bool): If True, will not apply any mitigation to highlight skew effects.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 50.
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
           aqe_enabled=False #To demonstrate this kind of skew
           shuffle_partitions=200 #Spark's default value

        logger.info("⚙️ Starting Scenario B demonstration")        

        # General configurations for Spark session
        configs = create_spark_configs_for_skew_demo(aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions)
        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)

        # Initialize Spark session
        spark = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark.sparkContext.setLogLevel("WARN")
        
        #Logs Spark session configs
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
        """
        logger.info("📊 Demonstrating skew behavior in Scenario B...")
        return df.groupBy("id").agg(F.count("*").alias("count"))

    def _solve_skew(self, df: DataFrame, spark: SparkSession):
        """
        Process data solving skew using dynamic salting and DataFrame split strategy.

        This method dynamically detects hot keys based on their percentage of total data,
        applies salting only to those keys, making the approach scalable even when there 
        are millions of hot keys.

        Args:
            df (DataFrame): Input DataFrame.
            spark (SparkSession): The SparkSession to estimate resources from.
        """
        logger.info("📦 Solving Skew in Scenario B with dynamic and scalable salting...")

        # --- Step 0: Estimate number of salt buckets ---
        total_rows = df.count()
        num_buckets = estimate_optimal_buckets(
            df=df,
            spark=spark,
            operation_type=OperationType.AGGREGATE
        )
        
        logger.info(f"Using {num_buckets} salt buckets for mitigation")

        # --- Step 1: Compute key distribution and persist it ---        
        key_distribution = (
            df.groupBy("id")
            .count()
            .withColumn("percentage", F.col("count") / F.lit(total_rows))
            .filter(F.col("percentage") > 0.05)  # Adjust threshold if needed
            .select("id")
        )

        # Persist hot keys in a broadcasted temp table or CTE
        key_distribution.createOrReplaceTempView("hot_keys_table")

        # --- Step 2: Split input DF into hot and cold partitions ---

        # Hot partition: rows where id is in hot_keys_table
        df_hot = df.join(
            spark.table("hot_keys_table").withColumnRenamed("id", "hot_id"),
            on=F.col("id") == F.col("hot_id"),
            how="inner"
        ).drop("hot_id")

        # Cold partition: all other rows
        df_cold = df.join(
            spark.table("hot_keys_table").withColumnRenamed("id", "hot_id"),
            on=F.col("id") == F.col("hot_id"),
            how="left_anti"
        )

        logger.info(f"Split completed: {df_hot.count()} hot records, {df_cold.count()} cold records")

        # --- Step 3: Apply salting only to hot partition ---
        df_hot_salted = df_hot.withColumn(
            "salt",
            F.monotonically_increasing_id() % num_buckets
        ).withColumn(
            "salted_id",
            F.concat(F.col("id"), F.lit(SALT_PREFIX), F.col("salt").cast("string"))
        )

        # Rename id -> salted_id to allow union later
        df_hot_salted = df_hot_salted.select(
            F.col("salted_id").alias("id"),
            F.col("value")
        )

        # --- Step 4: Repartition both datasets before aggregation ---
        df_hot_salted = df_hot_salted.repartition(num_buckets, "id")
        df_cold = df_cold.repartition("id")

        # --- Step 5: Aggregate each dataset separately ---
        df_hot_agg = df_hot_salted.groupBy("id").agg(F.count("*").alias("partial_count"))
        df_cold_agg = df_cold.groupBy("id").agg(F.count("*").alias("partial_count"))

        # Ensure both DataFrames have the same schema before union
        df_hot_agg = df_hot_agg.select("id", "partial_count")
        df_cold_agg = df_cold_agg.select(
            F.col("id").cast("string").alias("id"),
            F.col("partial_count").cast("long").alias("partial_count")
        )

        # Combine results safely
        df_combined = df_hot_agg.union(df_cold_agg)

        # --- Step 6: Remove salt and aggregate final counts ---
        df_final = (
            df_combined
            .withColumn("original_id", F.split(F.col("id"), SALT_PREFIX).getItem(0))
            .drop("id")
            .groupBy("original_id")
            .agg(F.sum("partial_count").alias("total_count"))
        )

        return df_final
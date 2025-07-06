"""
Scenario D7 Demonstration Module
Implements BaseDemonstration for Scenario D7: Skewed Join between customers and transactions.
Demonstrates how to mitigate data skew in join operations using dynamic salting when AQE is disabled.
"""

import logging
from typing import List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from py_spark_skew_solutions.demonstration.base_demonstration import BaseDemonstration
from py_spark_skew_solutions.utils.spark_utils import (
    create_or_get_spark_session,
    create_spark_configs_for_skew_demo,
    log_spark_config,
    get_executor_instances_number,
    stop_spark_session,
    read_parquet,
    save_dataframe_to_parquet
)
from py_spark_skew_solutions.utils.skew_utils import (
    estimate_optimal_buckets,    
    OperationType,
    SALT_PREFIX
)
from py_spark_skew_solutions.utils.common_utils import validate_required_params


logger = logging.getLogger(__name__)

class ScenarioD7Demonstration(BaseDemonstration):
    """
    Demonstration class for Scenario D7 (Skewed Join).
    This scenario simulates a dataset where most transactions belong to few customer_ids,
    causing skewed join behavior during customer-transaction joins.

    When AQE is enabled, Spark handles skew automatically via its skewedJoin optimization.
    When AQE is disabled, this class applies a salt-based strategy to mitigate the skew.
    """

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 200,
        **kwargs
    ) -> None:
        """
        Run the full demonstration workflow for Scenario D7.
        If AQE is enabled, no manual intervention is needed — Spark handles skew automatically.
        If AQE is disabled, we apply salting to spread out skewed customer_ids and reduce join bottleneck.

        Args:
            aqe_enabled (bool): Whether to enable Spark AQE optimizations.
            demonstrate_skew (bool): If True, will not apply any mitigation to highlight skew effects.
            shuffle_partitions (int): Number of shuffle partitions when AQE is disabled. Default: 200.
            **kwargs: Additional parameters for demonstration logic. Must include:
                - input_path (str): Path to read input data from.
                - output_path (str): Path to write results to.
        """
        # Validate required parameters
        validate_required_params(kwargs, required_keys=["input_path_transaction", "input_path_customer", "output_path"])

        # Retrieve parameteres from kwargs
        input_path_transaction = kwargs["input_path_transaction"]
        input_path_customer = kwargs["input_path_customer"]
        output_path = kwargs["output_path"]

        if demonstrate_skew:
            aqe_enabled = False
            shuffle_partitions = 200  # Spark's default

        logger.info("⚙️ Starting Scenario D7 demonstration")

        configs = create_spark_configs_for_skew_demo(aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions)
        # Removing autoBroadcastJoinThreshold to easily demonstrate Join Skew
        configs["spark.sql.autoBroadcastJoinThreshold"] = "-1"

        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)
        spark = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark.sparkContext.setLogLevel("WARN")
        log_spark_config(spark=spark)

        try:
            # Read input DataFrames
            logger.info(f"📂 Reading customer data from {input_path_customer}")
            logger.info(f"📂 Reading transaction data from {input_path_transaction}")
            customers_df = read_parquet(input_path=input_path_customer, spark=spark)
            transactions_df = read_parquet(input_path=input_path_transaction, spark=spark)

            if demonstrate_skew:
                result_df = self._demonstrate_skew(customers_df, transactions_df)
            else:
                result_df = self._solve_skew(customers_df, transactions_df, spark)

            save_dataframe_to_parquet(df=result_df, output_full_path=output_path)
        except Exception as e:
            logger.error(f"❌ Error during demonstration: {e}", exc_info=True)
            raise
        finally:
            stop_spark_session(spark=spark)

    def _demonstrate_skew(self, customers_df: DataFrame, transactions_df: DataFrame) -> DataFrame:
        """
        Simulate skewed join by performing inner join on customer_id.
        Args:
            customers_df (DataFrame): Customer dimension table.
            transactions_df (DataFrame): Transaction fact table with skewed distribution.
        Returns:
            DataFrame: Result of unmitigated skewed join.
        """
        logger.info("📊 Demonstrating skewed join in Scenario D7...")
        return transactions_df.join(customers_df, "customer_id", "inner")

    def _solve_skew(self, customers_df: DataFrame, transactions_df: DataFrame, spark: SparkSession) -> DataFrame:
        """
        Solve skewed join using dynamic salting based on key distribution.
        Applies salting only to hot customer_ids to evenly distribute load across executors.

        Args:
            customers_df (DataFrame): Customer dimension table.
            transactions_df (DataFrame): Transaction fact table with skewed distribution.
            spark (SparkSession): Active Spark session.

        Returns:
            DataFrame: Result of mitigated join.
        """
        logger.info("📦 Solving Skew in Scenario D7 with dynamic salting...")

        SKEWED_KEY_COL = "customer_id"
        SALTED_COL = f"{SALT_PREFIX}{SKEWED_KEY_COL}"

        total_rows = transactions_df.count()

        # Step 1: Estimate optimal number of buckets
        num_buckets = estimate_optimal_buckets(
            df=transactions_df,
            spark=spark,
            operation_type=OperationType.SHUFFLE_HEAVY,
            total_rows=total_rows
        )
        logger.info(f"Using {num_buckets} salt buckets for mitigation")

        # Step 2: Identify skewed keys (top 5% or more)
        key_distribution = (
            transactions_df.groupBy(SKEWED_KEY_COL)
            .count()
            .withColumn("percentage", F.col("count") / F.lit(total_rows))
            .filter(F.col("percentage") > 0.05)
            .select(SKEWED_KEY_COL)
        )

        key_distribution.cache().createOrReplaceTempView("hot_keys_table")
        skewed_keys = spark.table("hot_keys_table")

        # Step 3: Split into hot and cold partitions
        df_hot = transactions_df.join(skewed_keys, on=SKEWED_KEY_COL, how="inner")
        df_cold = transactions_df.join(skewed_keys, on=SKEWED_KEY_COL, how="left_anti")

        # Step 4: Apply salting only to hot partition
        df_hot_salted = df_hot.withColumn(
            "salt",
            F.monotonically_increasing_id() % num_buckets
        ).withColumn(
            SALTED_COL,
            F.concat(F.col(SKEWED_KEY_COL).cast("string"), F.lit("_"), F.col("salt").cast("string"))
        ).drop("salt")

        df_hot_salted = df_hot_salted.select(SALTED_COL, *[col for col in df_hot_salted.columns if col != SKEWED_KEY_COL])
        df_cold = df_cold.withColumn(SALTED_COL, F.col(SKEWED_KEY_COL).cast("string"))

        # Step 5: Repartition both datasets
        df_hot_salted = df_hot_salted.repartition(num_buckets, SALTED_COL)

        # Estimate the optimal number of partitions for df_cold
        num_partitions_cold = get_executor_instances_number(spark=spark) * estimate_optimal_buckets(
            df=df_cold,
            spark=spark,
            operation_type=OperationType.SHUFFLE_HEAVY
        )
        logger.info(f"Using {num_partitions_cold} partitions for cold dataframe")
        # Repartition df_cold with the optimal number of partitions
        df_cold = df_cold.repartition(num_partitions_cold, SALTED_COL)

        # Step 6: Salt customers table accordingly
        customers_df_salted = customers_df.join(
            skewed_keys.withColumnRenamed(SKEWED_KEY_COL, "hot_customer_id"),
            on=F.col(SKEWED_KEY_COL) == F.col("hot_customer_id"),
            how="inner"
        ).drop("hot_customer_id").crossJoin(
            spark.range(num_buckets).withColumnRenamed("id", "salt_index")
        ).withColumn(
            SALTED_COL,
            F.concat(F.col(SKEWED_KEY_COL).cast("string"), F.lit("_"), F.col("salt_index").cast("string"))
        ).select(SALTED_COL, *[col for col in customers_df.columns])

        customers_df_normal = customers_df.join(
            skewed_keys.withColumnRenamed(SKEWED_KEY_COL, "hot_customer_id"),
            on=F.col(SKEWED_KEY_COL) == F.col("hot_customer_id"),
            how="left_anti"
        ).withColumn(SALTED_COL, F.col(SKEWED_KEY_COL).cast("string"))

        customers_final = customers_df_salted.union(customers_df_normal)

        # Step 7: Perform join on salted keys
        df_final = df_hot_salted.union(df_cold).join(customers_final, SALTED_COL, "inner").select("transaction_id", "customer_id")

        return df_final
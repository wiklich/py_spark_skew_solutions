"""
Scenario A Demonstration Module

Implements BaseDemonstration for Scenario A: Many Keys with Few Records.
Demonstrates shuffle behavior and performance with or without AQE.
"""

import logging
from pyspark.sql import DataFrame
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
from py_spark_skew_solutions.utils.common_utils import validate_required_params


# Configure logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ScenarioADemonstration(BaseDemonstration):
    """
    Demonstration class for Scenario A.

    This scenario simulates many keys with few records per key.
    It reads data from a Parquet path and performs a groupBy + count to simulate shuffle overhead.

    When AQE is disabled, the shuffle partition count is fixed to 3 to highlight skew effects.
    """

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 18,
        **kwargs
    ) -> None:
        """
        Run the full demonstration workflow for Scenario A.

        Args:
            aqe_enabled (bool): Whether to enable Adaptive Query Execution.
            demonstrate_skew (bool): Whether to apply skew mitigation strategy.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 18.
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

        logger.info("⚙️ Starting Scenario A demonstration")

        # General configurations for Spark session
        configs = create_spark_configs_for_skew_demo(aqe_enabled=aqe_enabled, shuffle_partitions=shuffle_partitions)
        app_name = self.get_app_name(demonstrate_skew=demonstrate_skew)

        # Initialize Spark session
        spark_session = create_or_get_spark_session(app_name=app_name, configs=configs)
        spark_session.sparkContext.setLogLevel("WARN")

        #Logs Spark session configs
        log_spark_config(spark=spark_session)

        try:            
            df = read_parquet(input_path=input_path, spark=spark_session)
            logger.info("✅ Input DataFrame loaded from %s", input_path)

            if demonstrate_skew:
                result = self._demonstrate_skew(df)
            else:
                result = self._solve_skew(df, aqe_enabled, shuffle_partitions)

            save_dataframe_to_parquet(df=result, output_full_path=output_path)                

        except Exception as e:
            logger.error("❌ Error during demonstration: %s", e, exc_info=True)
            raise
        finally:
            stop_spark_session(spark=spark_session)

    def _demonstrate_skew(self, df: DataFrame) -> DataFrame:
        """
        Simulate skew behavior by performing shuffle operation on skewed data.

        Args:
            df (DataFrame): Input DataFrame.
        """
        logger.info("📊 Demonstrating skew behavior in Scenario A...")

        return df.groupBy("id").agg(F.count("*").alias("count"))
   

    def _solve_skew(self, df: DataFrame, aqe_enabled: bool, shuffle_partitions: int):
        """
        Process data solving skew.

        Args:
            df (DataFrame): Input DataFrame.
            aqe_enabled (bool): Whether AQE is enabled.
            shuffle_partitions (int): Number of shuffle partitions when AQE is disabled.
        """
        logger.info("📦 Solving Skew in Scenario A...")

        if not aqe_enabled:
            df = df.coalesce(shuffle_partitions)
            logger.info("⚙️ Coalescing DataFrame to %d partitions due to AQE being disabled", shuffle_partitions)

        return df.groupBy("id").agg(F.count("*").alias("count"))        
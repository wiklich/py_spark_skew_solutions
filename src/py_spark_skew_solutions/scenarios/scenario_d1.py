"""
Scenario D1: High Cardinality with Uneven Distribution

Generates data with many distinct keys where some are disproportionately frequent.
"""

import logging
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioD1(ScenarioBase):
    """
    Scenario D1 represents a hidden skew pattern where there's high cardinality,
    but a few keys appear much more frequently than others.

    This mimics real-world use cases such as customer activity logs or transaction data,
    where a small percentage of entities generate a large portion of the traffic.

    Ideal for testing strategies that detect and mitigate subtle forms of skew.
    """

    id = "D1"
    name = "High Cardinality with Uneven Distribution"
    description = "Many keys but some are disproportionately frequent."
    skew_type = "Hidden Skew"
    example = "'customer_id' with millions of customers, but only a few very active."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a DataFrame with many distinct keys, where one key appears significantly more often.

        Args:
            **kwargs: Optional parameters:
                - n_keys (int): Total number of distinct keys. Default: 1,000,000.
                - heavy_key (str): The key to be skewed. Default: "VIP".
                - heavy_rows (int): Number of rows assigned to the heavy key. Default: 100,000.

        Returns:
            DataFrame: Generated DataFrame with schema ['id', 'value'].
        """
        n_keys = kwargs.get("n_keys", 1_000_000)
        heavy_key = kwargs.get("heavy_key", "VIP")
        heavy_rows = kwargs.get("heavy_rows", 100_000)

        logger.info(
            "Generating skewed DataFrame with parameters: "
            f"n_keys={n_keys}, heavy_key='{heavy_key}', heavy_rows={heavy_rows}"
        )

        df = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            n_keys=n_keys,
            heavy_key=heavy_key,
            heavy_rows=heavy_rows
        )[0]

        logger.info("DataFrame successfully generated with schema: %s", df.schema.simpleString())
        return df

    def detect_skew(self, **kwargs) -> bool:
        """
        Detects skew by analyzing key distribution across partitions.

        Args:
            **kwargs: Optional parameters for skew detection.

        Returns:
            bool: Always returns False for this scenario (no significant skew expected).
        """
        result = detect(scenario_id=self.id, **kwargs)
        print(f"Scenario {self.id}: {'⚠️ Skew Detected' if result else '✅ No Skew'}")
        return result

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 200,
        **kwargs
    ) -> None:
        """
        Executes a full demonstration of Scenario D1.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 200.
            **kwargs: Additional parameters for demonstration logic. Must include:
                - input_path (str): Path to read input data from.
                - output_path (str): Path to write results to.
        """
        logger.info("Starting demonstration for scenario '%s' (%s)", self.name, self.id)
        run_demonstration(
            scenario_id=self.id,
            aqe_enabled=aqe_enabled,
            demonstrate_skew=demonstrate_skew,
            shuffle_partitions=shuffle_partitions,
            **kwargs
        )
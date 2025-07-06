"""
Scenario A: Generates a DataFrame with many keys but few records per key.
"""

import logging
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioA(ScenarioBase):
    """
    Scenario A generates a DataFrame with a high number of distinct keys,
    each with few associated records. This pattern can lead to overhead during shuffling.

    Ideal for testing partitioning strategies and shuffle performance.
    """

    id = "A"
    name = "Many Keys with Few Records"
    description = "High number of distinct keys, each with few records."
    skew_type = "Overhead"
    example = "'id' column with 2 million unique keys, each associated with one row."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a DataFrame with many unique keys and minimal records per key.

        Args:
            **kwargs: Accepts `n_keys` as an optional parameter.
                      Default: 2,000,000 keys.

        Returns:
            DataFrame: Generated DataFrame with columns ['id', 'value'].
        """
        n_keys = kwargs.get("n_keys", 2_000_000)
        logger.info("Generating DataFrame with %d keys", n_keys)
        df = run_scenario(scenario_id=self.id, spark=self.spark, n_keys=n_keys)[0]
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
        shuffle_partitions: int = 18,
        **kwargs
    ) -> None:
        """
        Executes a full demonstration of Scenario A.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 18.
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

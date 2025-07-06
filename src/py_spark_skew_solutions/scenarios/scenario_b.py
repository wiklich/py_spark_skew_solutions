"""
Scenario B: Generates a DataFrame with a few keys where one key dominates (classic skew pattern).
"""

import logging
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration

# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioB(ScenarioBase):
    """
    Scenario B generates a DataFrame with a small number of distinct keys,
    where one key dominates the dataset in terms of record count.

    This pattern represents classic data skew and is ideal for testing
    skew mitigation strategies such as salting or custom partitioning.
    """

    id = "B"
    name = "Few Keys with Many Records"
    description = "One or few keys dominate the dataset."
    skew_type = "Classic Skew"
    example = "'id' column with one key ('HOT') repeated 2 million times and 10,000 other keys with one row each."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a skewed DataFrame with one dominant key and many smaller ones.

        Args:
            **kwargs: Optional parameters:
                - heavy_key (str): The key to be skewed. Default: "HOT".
                - heavy_rows (int): Number of rows assigned to the heavy key. Default: 2,000,000.
                - other_keys (int): Number of additional keys. Default: 10,000.

        Returns:
            DataFrame: Generated DataFrame with columns ['id', 'value'].
        """
        heavy_key = kwargs.get("heavy_key", "HOT")
        heavy_rows = kwargs.get("heavy_rows", 2_000_000)
        other_keys = kwargs.get("other_keys", 10_000)

        logger.info(
            "Generating skewed DataFrame with parameters: "
            f"heavy_key='{heavy_key}', heavy_rows={heavy_rows}, other_keys={other_keys}"
        )

        df = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            heavy_key=heavy_key,
            heavy_rows=heavy_rows,
            other_keys=other_keys
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
        shuffle_partitions: int = 50,
        **kwargs
    ) -> None:
        """
        Executes a full demonstration of Scenario B.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 50.
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
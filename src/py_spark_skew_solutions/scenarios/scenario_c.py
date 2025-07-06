"""
Scenario C: Combines characteristics from Scenarios A and B — many small keys with a few dominant ones.
"""

import logging
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration

# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioC(ScenarioBase):
    """
    Scenario C combines the characteristics of both Scenario A and B:
    - Many small keys with few records per key (like A)
    - One or more dominant keys with high record count (like B)

    This mixed skew pattern represents real-world scenarios where multiple skew types coexist,
    making it challenging to apply uniform mitigation strategies.

    Ideal for testing advanced skew mitigation techniques.
    """

    id = "C"
    name = "Combination A + B scenarios"
    description = "Mix of many small keys and few dominant ones."
    skew_type = "Mixed Skew"
    example = "'id' column with 2 million keys having one row each, plus key 'HOT' repeated 200,000 times."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a mixed-skew DataFrame containing:
        - A large number of small keys
        - One or more heavy keys with significantly more records

        Args:
            **kwargs: Optional parameters:
                - n_small_keys (int): Number of small keys with few records. Default: 2,000,000.
                - heavy_key (str): Key with high volume of data. Default: "HOT".
                - heavy_rows (int): Number of rows assigned to the heavy key. Default: 200,000.

        Returns:
            DataFrame: Generated DataFrame with columns ['id', 'value'].
        """
        n_small_keys = kwargs.get("n_small_keys", 2_000_000)
        heavy_key = kwargs.get("heavy_key", "HOT")
        heavy_rows = kwargs.get("heavy_rows", 200_000)

        logger.info(
            "Generating mixed-skew DataFrame with parameters: "
            f"n_small_keys={n_small_keys}, heavy_key='{heavy_key}', heavy_rows={heavy_rows}"
        )

        df = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            n_small_keys=n_small_keys,
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
        shuffle_partitions: int = 50,
        **kwargs
    ) -> None:
        """
        Executes a full demonstration of Scenario C.

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
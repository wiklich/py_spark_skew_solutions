"""
Scenario D3: Temporal Skew with Hourly Peaks

Generates a DataFrame where data is skewed towards a specific hour of the day.
"""

import logging
from datetime import datetime
from typing import Optional
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioD3(ScenarioBase):
    """
    Scenario D3 generates time-series data with skew concentrated in a specific hour of the day.

    This pattern represents real-world scenarios such as:
    - Online purchases during morning business hours
    - Batch job executions at midnight
    - User login peaks at start of workday

    Ideal for testing time-based partitioning strategies and shuffle behavior under temporal skew.
    """

    id = "D3"
    name = "Temporal Skew with Hourly Peaks"
    description = "Skewed distribution of records across time, with one hour dominating."
    skew_type = "Temporal Skew"
    example = "'event_time' with most rows generated around 9 AM daily."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a DataFrame with timestamps skewed toward a specific hour each day.

        Args:
            **kwargs: Optional parameters:
                - start (datetime, optional): Start date for event generation. Default: today at midnight.
                - days (int): Number of days to generate data for. Default: 1.
                - heavy_hour (int): The hour of the day (0-23) with peak volume. Default: 9.
                - heavy_rows (int): Rows assigned to the heavy hour. Default: 1,200,000.
                - other_rows (int): Rows distributed across other hours. Default: 800,000.

        Returns:
            DataFrame: Generated DataFrame with schema ['event_time', 'value']
        """
        start: Optional[datetime] = kwargs.get("start", datetime.now().replace(hour=0, minute=0, second=0, microsecond=0))
        days: int = kwargs.get("days", 1)
        heavy_hour: int = kwargs.get("heavy_hour", 9)
        heavy_rows: int = kwargs.get("heavy_rows", 1_200_000)
        other_rows: int = kwargs.get("other_rows", 800_000)

        logger.info(
            "Generating time-based skewed DataFrame with parameters: "
            f"start={start}, days={days}, heavy_hour={heavy_hour}, "
            f"heavy_rows={heavy_rows}, other_rows={other_rows}"
        )

        df = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            start=start,
            days=days,
            heavy_hour=heavy_hour,
            heavy_rows=heavy_rows,
            other_rows=other_rows
        )

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
        Executes a full demonstration of Scenario D3.

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
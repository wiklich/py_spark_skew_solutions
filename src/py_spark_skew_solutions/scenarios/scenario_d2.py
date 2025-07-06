"""
Scenario D2: Skewed Data in Multi-Dimensional Keys

Generates a DataFrame with combinations of country and product,
where one specific combination dominates the dataset.
"""

import logging
from typing import List, Optional, Tuple
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioD2(ScenarioBase):
    """
    Scenario D2 generates data with multi-dimensional keys (e.g., country + product),
    where one specific combination has significantly more records than others.

    This pattern represents real-world use cases like sales data or user analytics,
    where certain combinations (e.g., 'BR' + 'X') dominate due to regional or behavioral factors.

    Ideal for testing skew mitigation strategies in dimensional joins or aggregations.
    """

    id = "D2"
    name = "Skewed Multi-Dimensional Key Combinations"
    description = "One key combination (e.g., country+product) dominates the dataset."
    skew_type = "Multi-Dimensional Skew"
    example = "'country' and 'product' with most rows concentrated on ('BR', 'X')."

    def generate_data(self, **kwargs) -> List[DataFrame]:
        """
        Generate a DataFrame with multiple country-product combinations,
        where one pair is heavily skewed.

        Args:
            **kwargs: Optional parameters:
                - countries (List[str], optional): List of countries to include. Default: ["BR", "US", "FR", "IN"].
                - products (List[str], optional): List of products to include. Default: ["X", "Y", "Z"].
                - heavy_combo (Tuple[str, str]): The dominant country-product combination. Default: ("BR", "X").
                - heavy_rows (int): Number of rows assigned to the heavy combination. Default: 1,600,000.
                - other_rows (int): Rows distributed across other combinations. Default: 400,000.

        Returns:
            List[DataFrame]: A list containing one DataFrame with schema ['country', 'product', 'value'].
        """
        countries: Optional[List[str]] = kwargs.get("countries", ["BR", "US", "CA", "DE", "FR"])
        products: Optional[List[str]] = kwargs.get("products", ["X", "Y", "Z"])
        heavy_combo: Tuple[str, str] = kwargs.get("heavy_combo", ("BR", "X"))
        heavy_rows: int = kwargs.get("heavy_rows", 1_600_000)
        other_rows: int = kwargs.get("other_rows", 400_000)

        logger.info(
            "Generating multi-dimensional DataFrame with parameters: "
            f"countries={countries}, products={products}, heavy_combo={heavy_combo}, "
            f"heavy_rows={heavy_rows}, other_rows={other_rows}"
        )

        df_list = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            countries=countries,
            products=products,
            heavy_combo=heavy_combo,
            heavy_rows=heavy_rows,
            other_rows=other_rows
        )

        logger.info("DataFrame successfully generated with schema: %s", df_list[0].schema.simpleString())
        return df_list

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
        Executes a full demonstration of Scenario D2.

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
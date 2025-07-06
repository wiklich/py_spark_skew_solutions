"""
Scenario D2: Generates a DataFrame with specific combinations of columns concentrate most data.
"""

import random
from typing import List, Tuple
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioD2DataGenerator(BaseScenarioDataGenerator):
    """
    Generates data with skew in a composite key (country + product).
    Useful for testing joins and aggregations on multi-key scenarios.
    """

    def generate(self, countries: List[str] | None = None, products: List[str] | None = None,
                 heavy_combo: Tuple[str, str] = ("BR", "X"),
                 heavy_rows: int = 1_600_000, other_rows: int = 400_000) -> List[DataFrame]:
        """
        Generate skewed data across a composite key.

        Parameters:
            countries (List[str]): List of country codes.
            products (List[str]): List of product codes.
            heavy_combo (Tuple[str, str]): Composite key with most data.
            heavy_rows (int): Rows for the heavy combo.
            other_rows (int): Rows for other combinations.

        Returns:
            DataFrame: Spark DataFrame with columns ['country', 'product', 'qty'].
        """
        countries = countries or ["BR", "US", "CA", "DE", "FR"]
        products = products or ["X", "Y", "Z"]

        rows = []
        # Heavy combo
        rows += [{"country": heavy_combo[0], "product": heavy_combo[1], "qty": i} for i in range(heavy_rows)]
        # Other combos evenly distributed
        for _ in range(other_rows):
            c = random.choice(countries)
            p = random.choice(products)
            if (c, p) == heavy_combo:
                continue
            rows.append({"country": c, "product": p, "qty": random.randint(1, 10)})        
        df = require_active_spark_session().createDataFrame(rows)
        return [df]
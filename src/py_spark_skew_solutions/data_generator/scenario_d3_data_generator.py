"""
Scenario D3: Generates a DataFrame with certain time periods have much higher volume.
"""

import random
from typing import List
from datetime import datetime, timedelta
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

class ScenarioD3DataGenerator(BaseScenarioDataGenerator):
    """
    Generates temporal data skewed at a specific hour.
    Useful for time-based analysis and windowing operations.
    """

    def generate(self, start: datetime | None = None, days: int = 1,
                 heavy_hour: int = 9, heavy_rows: int = 1_200_000,
                 other_rows: int = 800_000) -> List[DataFrame]:
        """
        Generate data with a peak during a specific hour.

        Parameters:
            start (datetime): Start date for timestamps.
            days (int): Number of days to span data over.
            heavy_hour (int): Hour with most activity.
            heavy_rows (int): Number of rows during the heavy hour.
            other_rows (int): Rows spread across other hours.

        Returns:
            List[DataFrame]: A list containing one DataFrame with columns ['id', 'ts'].
        """
        start = start or datetime(2025, 1, 1)
        base_ts = start.replace(hour=heavy_hour, minute=0, second=0, microsecond=0)

        rows = [{"id": i, "ts": base_ts} for i in range(heavy_rows)]

        for i in range(other_rows):
            rand_hour = random.choice([h for h in range(24) if h != heavy_hour])
            rand_day = random.randint(0, days - 1)
            ts = start.replace(hour=rand_hour, minute=0, second=0, microsecond=0)
            ts += timedelta(days=rand_day, minutes=random.randint(0, 59))
            rows.append({
                "id": heavy_rows + i,
                "ts": ts
            })        
        df = require_active_spark_session().createDataFrame(rows)
        return [df]
"""
Skew Detector for Scenario D3: Temporal skew
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import DEFAULT_THRESHOLDS
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioD3Detector(BaseScenarioDetector):
    """
    Detects Scenario D3: Skew concentrated in specific time windows.

    Ideal for event-based datasets where activity peaks at certain hours or days.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        """
        Analyze temporal distribution to detect skewed buckets.

        Args:
            df (DataFrame): Input DataFrame with timestamp column.

        Returns:
            bool: True if temporal skew detected.
        """
        validate_required_params(kwargs, required_keys=["timestamp_col"])
        timestamp_col = kwargs["timestamp_col"]


        exists_timestamp_col = timestamp_col in df.columns
        if not exists_timestamp_col:
            return False
        heavy_key_ratio = self.thresholds.get("heavy_key_ratio", DEFAULT_THRESHOLDS["heavy_key_ratio"])
        bucket = self.thresholds.get("temporal_bucket", DEFAULT_THRESHOLDS["temporal_bucket"])
        bucketed = df.withColumn("time_bucket", F.date_trunc(bucket, F.col(timestamp_col)))
        counts_df = bucketed.groupBy("time_bucket").count()
        total = df.count()
        max_cnt = counts_df.agg(F.max("count")).first()[0]
        return max_cnt / total >= heavy_key_ratio
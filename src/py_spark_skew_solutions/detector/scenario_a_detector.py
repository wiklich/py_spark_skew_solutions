"""
Skew Detector for Scenario A: Many keys with few records per key.
"""

from pyspark.sql import DataFrame
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import _basic_metrics
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioADetector(BaseScenarioDetector):
    """
    Detects Scenario A: High number of distinct keys with few rows per key.

    This pattern causes shuffle overhead but does not produce significant data imbalance.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        metrics = _basic_metrics(df, key_cols)

        distinct_ratio_high = self.thresholds.get("distinct_ratio_high", 0.8)
        small_group_max = self.thresholds.get("small_group_max", 10)

        return (
            metrics["distinct"] / metrics["total"] >= distinct_ratio_high and
            metrics["max"] <= small_group_max
        )
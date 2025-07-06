"""
Skew Detector for Scenario D1: High cardinality with uneven distribution
"""

from pyspark.sql import DataFrame
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import _basic_metrics, DEFAULT_THRESHOLDS
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioD1Detector(BaseScenarioDetector):
    """
    Detects Scenario D1: Many keys, but some are disproportionately frequent.

    Common in customer activity logs or transaction data.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        metrics = _basic_metrics(df, key_cols)

        distinct_ratio_high = self.thresholds.get("distinct_ratio_high", DEFAULT_THRESHOLDS["distinct_ratio_high"]) / 2
        heavy_key_ratio = self.thresholds.get("heavy_key_ratio", DEFAULT_THRESHOLDS["heavy_key_ratio"])

        return (
            metrics["distinct"] / metrics["total"] >= distinct_ratio_high and
            metrics["max"] / metrics["total"] >= heavy_key_ratio
        )
"""
Skew Detector for Scenario B: Few keys with many records.
"""

from pyspark.sql import DataFrame
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import DEFAULT_THRESHOLDS, _basic_metrics
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioBDetector(BaseScenarioDetector):
    """
    Detects Scenario B: One or few keys dominate the dataset.

    This pattern causes classic data skew and leads to long-running tasks during shuffles.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        metrics = _basic_metrics(df, key_cols)

        distinct_ratio_low = self.thresholds.get("distinct_ratio_low", DEFAULT_THRESHOLDS["distinct_ratio_low"])
        dominant_key_ratio = self.thresholds.get("dominant_key_ratio", DEFAULT_THRESHOLDS["dominant_key_ratio"])

        return (
            metrics["distinct"] / metrics["total"] <= distinct_ratio_low or
            metrics["max"] / metrics["total"] >= dominant_key_ratio
        )
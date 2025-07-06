"""
Skew Detector for Scenario D7: Join Skew
"""

from pyspark.sql import DataFrame
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import _basic_metrics
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioD7Detector(BaseScenarioDetector):
    """
    Detects Scenario D7: Join Skew where most transactions belong to a few customer_ids,
    causing uneven task load during join operations.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        metrics = _basic_metrics(df, key_cols)        

        dominant_key_ratio = self.thresholds.get("dominant_key_ratio", 0.5)
        heavy_key_ratio = self.thresholds.get("heavy_key_ratio", 0.05)

        # Either one key dominates over half of the dataset...
        max_ratio = metrics["max"] / metrics["total"]
        # Or a small subset (e.g., top 5%) contributes significantly
        tail_ratio = metrics["p99"] / metrics["avg"]

        return max_ratio >= dominant_key_ratio or tail_ratio >= heavy_key_ratio
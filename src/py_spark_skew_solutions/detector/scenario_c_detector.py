"""
Skew Detector for Scenario C: Mixed Skew
"""

from pyspark.sql import DataFrame
from py_spark_skew_solutions.detector.scenario_a_detector import ScenarioADetector
from py_spark_skew_solutions.detector.scenario_b_detector import ScenarioBDetector
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import DEFAULT_THRESHOLDS, _basic_metrics
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioCDetector(BaseScenarioDetector):
    """
    Detects Scenario C: Mixed skew pattern with both high-cardinality and dominant keys.

    This pattern combines elements from A and B, but doesn't match either exactly.
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        metrics = _basic_metrics(df, key_cols)

        a_detector = ScenarioADetector(self.thresholds)
        b_detector = ScenarioBDetector(self.thresholds)

        if a_detector.detect(df, **kwargs) or b_detector.detect(df, **kwargs):
            return False

        heavy_tail_factor = self.thresholds.get("heavy_tail_factor", DEFAULT_THRESHOLDS["heavy_tail_factor"])
        return metrics["max"] / metrics["avg"] >= heavy_tail_factor
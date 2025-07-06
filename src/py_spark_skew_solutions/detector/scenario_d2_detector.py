"""
Skew Detector for Scenario D2: Multi-dimensional key combination skew
"""


from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.skew_detection_utils import DEFAULT_THRESHOLDS, _group_counts
from py_spark_skew_solutions.utils.common_utils import validate_required_params


class ScenarioD2Detector(BaseScenarioDetector):
    """
    Detects Scenario D2: Skewed combinations of multi-dimensional keys.

    Useful for detecting imbalance in joins across composite keys like (country, product).
    """

    def detect(self, df: DataFrame, **kwargs) -> bool:
        validate_required_params(kwargs, required_keys=["key_cols"])
        key_cols = kwargs["key_cols"]
        
        if len(key_cols) < 2:
            return False
        
        heavy_key_ratio = self.thresholds.get("heavy_key_ratio", DEFAULT_THRESHOLDS["heavy_key_ratio"])
        
        counts_df = _group_counts(df, key_cols)
        total = df.count()
        max_cnt = counts_df.agg(F.max("count")).first()[0]
        return max_cnt / total >= heavy_key_ratio

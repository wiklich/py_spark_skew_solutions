"""
Module for detecting all known skew scenarios using registered detectors.
"""

import logging
from typing import Dict
from pyspark.sql import DataFrame

from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector
from py_spark_skew_solutions.detector.scenario_detector_factory import get_detector, SCENARIO_DETECTOR_REGISTRY
from py_spark_skew_solutions.detector.skew_detection_utils import DEFAULT_THRESHOLDS, _basic_metrics
from py_spark_skew_solutions.utils.common_utils import validate_required_params


# Configure logger for this module
logger = logging.getLogger(__name__)


class AllScenarioDetector(BaseScenarioDetector):
    """
    Runs all available skew scenario detectors and returns results in a unified dictionary.

    Usage:
        detector = AllScenarioDetector()
        result = detector.detect(df, key_cols=["id"])
        detector.print_report(result)
    """

    def __init__(self):
        self.supported_scenarios = list(SCENARIO_DETECTOR_REGISTRY.keys())
        logger.info("Initialized AllScenarioDetector with supported scenarios: %s", self.supported_scenarios)

    def detect(
        self,
        df: DataFrame,
        **kwargs
    ) -> Dict[str, bool]:
        """
        Detects all supported skew patterns in the given DataFrame.

        Args:
            df (DataFrame): Input DataFrame to analyze.
            **kwargs: Accepts `n_keys` as an optional parameter.

        Returns:
            Dict[str, bool]: Dictionary mapping scenario IDs to detection status.
        """
        logger.info("Starting skew detection for all scenarios")
    
        validate_required_params(kwargs, required_keys=["key_cols", "timestamp_col"])
        key_cols = kwargs["key_cols"]
        timestamp_col = kwargs["timestamp_col"]
        thresholds = None

        t = {**DEFAULT_THRESHOLDS, **(thresholds or {})}

        # Run basic metrics once
        metrics = _basic_metrics(df, key_cols)
        logger.debug("Basic metrics computed: %s", metrics)

        results: Dict[str, bool] = {}

        for scenario_id in self.supported_scenarios:
            try:
                logger.info("Detecting skew for scenario: %s", scenario_id)
                detector = get_detector(scenario_id, thresholds=t)
                detected = detector.detect(df, **kwargs)
                results[scenario_id] = detected
                logger.info("Scenario %s: Skew %s", scenario_id, "detected" if detected else "not detected")
            except KeyError as e:
                logger.error("Error loading detector for scenario %s: %s", scenario_id, e)

        logger.info("Skew detection completed. Results: %s", results)
        return results

    def print_report(self, results: Dict[str, bool]) -> None:
        """
        Prints a formatted report of detected skew patterns.

        Args:
            results (Dict[str, bool]): Skew detection results from `detect`.
        """
        logger.info("Printing skew detection report")
        print("\n===== 🧪 SKEW DETECTION REPORT =====")
        for scenario_id, is_skewed in sorted(results.items()):
            status = "⚠️  Skew detected" if is_skewed else "✅  OK"
            print(f"{scenario_id}:\t{status}")
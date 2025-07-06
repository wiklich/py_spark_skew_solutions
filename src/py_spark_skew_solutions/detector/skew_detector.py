"""
Module for detecting skew in Parquet datasets using registered scenario detectors.
"""

import logging

from py_spark_skew_solutions.detector.scenario_detector_factory import get_detector
from py_spark_skew_solutions.utils.spark_utils import create_or_get_spark_session
from py_spark_skew_solutions.utils.common_utils import validate_required_params


# Configure logger for this module
logger = logging.getLogger(__name__)


def detect(
    scenario_id: str,
    **kwargs
) -> bool:
    """
    Detects a specific skew pattern in the dataset located at input_path.

    Args:
        scenario_id (str): ID of the scenario to detect (e.g., "A", "D7").
        **kwargs: Optional parameters for skew detection.

    Returns:
        bool: True if skew is detected, False otherwise.
    """
    validate_required_params(kwargs, required_keys=["input_path"])
    # Retrieve input_path parameter from kwargs
    input_path = kwargs["input_path"]
    logger.info("Starting skew detection for scenario %s on path: %s", scenario_id, input_path)

    spark = create_or_get_spark_session(f"Skew Detector - Scenario {scenario_id}")
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(input_path)
    logger.info("Loaded DataFrame with schema: %s", df.schema.simpleString())

    detector = get_detector(scenario_id=scenario_id)
    logger.info("Using detector class: %s", type(detector).__name__)

    result = detector.detect(df, **kwargs)
    logger.info("Detection completed. Skew detected: %s", "Yes" if result else "No")

    return result
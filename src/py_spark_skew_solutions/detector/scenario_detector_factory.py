"""
Factory module for creating scenario detectors.
"""

from typing import Type, Dict, Optional
from py_spark_skew_solutions.detector.base_scenario_detector import BaseScenarioDetector


SCENARIO_DETECTOR_REGISTRY: Dict[str, Type[BaseScenarioDetector]] = {}


def register_detector(scenario_id: str, detector_class: Type[BaseScenarioDetector]) -> None:
    """
    Registers a detector class under the given ID for later retrieval.

    Args:
        scenario_id (str): Unique identifier for the scenario.
        detector_class (Type[BaseScenarioDetector]): The detector class to register.
    """
    SCENARIO_DETECTOR_REGISTRY[scenario_id] = detector_class


def get_detector(scenario_id: str, thresholds: Optional[Dict[str, float]] = None) -> BaseScenarioDetector:
    """
    Retrieves an instance of a scenario detector by its ID.

    Args:
        scenario_id (str): The ID of the scenario detector to retrieve.
        thresholds (Optional[Dict[str, float]]): Custom thresholds for detection logic.

    Returns:
        BaseScenarioDetector: An instance of the requested scenario detector.

    Raises:
        KeyError: If no detector is registered with the provided ID.
    """
    cls = SCENARIO_DETECTOR_REGISTRY.get(scenario_id)
    if cls is None:
        available_detectors = list(SCENARIO_DETECTOR_REGISTRY.keys())
        raise KeyError(
            f"Detector for scenario '{scenario_id}' not found. "
            f"Available scenarios: {available_detectors}"
        )
    return cls(thresholds=thresholds)
"""
Abstract base class for all scenario detectors.
"""

from abc import ABC, abstractmethod
from pyspark.sql import DataFrame
from typing import Optional, Dict


class BaseScenarioDetector(ABC):
    """
    Abstract base class for all skew scenario detectors.

    Subclasses must implement the following method:
        - detect: Analyzes a DataFrame and returns True if the scenario's skew pattern is detected.
    """

    def __init__(self, thresholds: Optional[Dict[str, float]] = None):
        self.thresholds = thresholds or {}

    @abstractmethod
    def detect(self, df: DataFrame, **kwargs) -> bool:
        """
        Detects if the given DataFrame matches this scenario's skew pattern.

        Args:
            df (DataFrame): The DataFrame to analyze.
            **kwargs: Optional additional parameters for detection logic.

        Returns:
            bool: True if skew pattern is detected.
        """
        raise NotImplementedError("Subclasses must implement the detect method.")
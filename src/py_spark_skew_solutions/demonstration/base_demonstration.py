"""
Base module for demonstration classes.

This module defines an abstract base class for all skew scenario demonstrations.
Each subclass must implement the 'run_demonstration' method with standardized parameters.
"""

from abc import ABC, abstractmethod
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase


class BaseDemonstration(ABC):
    """
    Abstract base class for demonstrating and mitigating data skew scenarios.

    Subclasses must implement the following method:
        - run_demonstration: Executes the full demonstration workflow

    Attributes:
        spark (SparkSession): Active Spark session used across the demonstration
    """

    def __init__(self, scenario: ScenarioBase):
        """
        Initializes the base demonstration with a SparkSession.

        Args:
            scenario (ScenarioBase):
        """
        self.scenario = scenario
        

    def get_app_name(self, demonstrate_skew: bool) -> str:
        """
        Returns a formatted application name for the SparkSession.

        Args:
            demonstrate_skew (bool): If True, indicates that the demonstration will show skew behavior.
                                     If False, indicates that it's solving or analyzing skew.

        Returns:
            str: Formatted application name for logging and tracking purposes.
        """
        return f"Demo Skew Scenario {self.scenario.id} - {'Demonstrating Skew' if demonstrate_skew else 'Solving Skew'}"
        

    @abstractmethod
    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 18,
        **kwargs
    ) -> None:
        """
        Execute the full demonstration workflow.

        This method must be implemented by subclasses to perform either generation or mitigation steps.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 18.
            **kwargs: Additional parameters required for the specific demonstration (e.g., input_path, output_path).

        Returns:
            None
        """
        raise NotImplementedError("Subclasses must implement the 'run_demonstration' method.")
"""
Factory module for creating demonstration instances.

This module provides utilities to register and retrieve demonstration classes
that inherit from BaseDemonstration.
"""

from typing import Type, Dict
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.demonstration.base_demonstration import BaseDemonstration
from py_spark_skew_solutions.scenarios.scenario_factory import get_scenario


# Registry mapping scenario IDs to their corresponding demonstration classes
DEMONSTRATION_REGISTRY: Dict[str, Type[BaseDemonstration]] = {}


def register_demonstration(scenario_id: str, demonstration_class: Type[BaseDemonstration]) -> None:
    """
    Register a demonstration class under the given scenario ID.

    Args:
        scenario_id (str): The ID of the scenario to associate with this demonstration.
        demonstration_class (Type[BaseDemonstration]): Class that implements BaseDemonstration.
    """
    DEMONSTRATION_REGISTRY[scenario_id] = demonstration_class


def get_demonstration(scenario_id: str, scenario: ScenarioBase) -> BaseDemonstration:
    """
    Retrieve and instantiate a demonstration by its scenario ID.

    Args:
        scenario_id (str): ID of the scenario to demonstrate.
        scenario (ScenarioBase): Scenario instance to inject into the demonstration.

    Returns:
        BaseDemonstration: An instance of the requested demonstration class.

    Raises:
        KeyError: If no demonstration is registered for the provided scenario_id.
    """
    demonstration_class = DEMONSTRATION_REGISTRY.get(scenario_id)
    if demonstration_class is None:
        available = list(DEMONSTRATION_REGISTRY.keys())
        raise KeyError(
            f"No demonstration found for scenario '{scenario_id}'. "
            f"Available scenarios: {available}"
        )
    return demonstration_class(scenario=scenario)


def run_demonstration(
    scenario_id: str,
    aqe_enabled: bool,
    demonstrate_skew: bool,
    shuffle_partitions: int = 18,
    **kwargs
) -> None:
    """
    Run a scenario by ID 

    Args:
        scenario_id (str): ID of the scenario to demonstrate.
        aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
        demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
        shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 18.
        **kwargs: Additional parameters required for the specific demonstration (e.g., input_path, output_path).
    """
    scenario = get_scenario(scenario_id=scenario_id, spark=None)
    scenario_demonstration = get_demonstration(scenario_id=scenario_id, scenario=scenario)

    scenario_demonstration.run_demonstration(
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,
        shuffle_partitions=shuffle_partitions,
        **kwargs
    )
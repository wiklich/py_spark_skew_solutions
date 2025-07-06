"""
Scenario Factory Module

This module provides utilities for dynamically registering and retrieving scenario classes
that implement the ScenarioBase interface. It uses a registry pattern to allow extensible
and decoupled scenario management.
"""

from typing import Type, Dict
from pyspark.sql import SparkSession
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase


# Global registry mapping scenario IDs to their corresponding implementation classes
SCENARIO_REGISTRY: Dict[str, Type[ScenarioBase]] = {}


def register_scenario(scenario_id: str, scenario_class: Type[ScenarioBase]) -> None:
    """
    Registers a scenario class under the given ID for later retrieval.

    Args:
        scenario_id (str): Unique identifier for the scenario.
        scenario_class (Type[ScenarioBase]): The scenario class to register.

    Raises:
        TypeError: If the provided class does not inherit from ScenarioBase.
    """
    if not issubclass(scenario_class, ScenarioBase):
        raise TypeError(f"Class {scenario_class.__name__} must inherit from ScenarioBase")

    SCENARIO_REGISTRY[scenario_id] = scenario_class


def get_scenario(scenario_id: str, spark: SparkSession) -> ScenarioBase:
    """
    Retrieves an instantiated scenario object by its ID.

    Args:
        scenario_id (str): The ID of the scenario to retrieve.
        spark (SparkSession): An active Spark session to be passed to the scenario constructor.

    Returns:
        ScenarioBase: An instance of the requested scenario class.

    Raises:
        KeyError: If no scenario is registered with the provided ID.
    """
    scenario_class = SCENARIO_REGISTRY.get(scenario_id)
    if scenario_class is None:
        available_scenarios = list(SCENARIO_REGISTRY.keys())
        raise KeyError(
            f"Scenario '{scenario_id}' not found in registry. "
            f"Available scenarios: {available_scenarios}"
        )
    return scenario_class(spark)
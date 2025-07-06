"""
Factory module for creating scenario data generators.
"""

from typing import List, Type, Dict
from pyspark.sql import SparkSession, DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator


# Registry of available scenarios
SCENARIO_REGISTRY: Dict[str, Type[BaseScenarioDataGenerator]] = {}


def register_scenario(scenario_id: str, generator_class: Type[BaseScenarioDataGenerator]) -> None:
    """
    Register a new scenario generator class under the given ID.

    Args:
        scenario_id (str): ID to associate with the generator.
        generator_class (Type[BaseScenarioDataGenerator]): Class of the generator to register.
    """
    SCENARIO_REGISTRY[scenario_id] = generator_class


def get_scenario_generator(scenario_id: str, spark: SparkSession) -> BaseScenarioDataGenerator:
    """
    Factory method to retrieve and instantiate a scenario generator by ID.

    Args:
        scenario_id (str): The ID of the scenario to retrieve.
        spark (SparkSession): Active Spark session.

    Returns:
        BaseScenarioDataGenerator: An instance of the requested scenario generator.

    Raises:
        KeyError: If the scenario ID is not found in the registry.
    """
    generator_class = SCENARIO_REGISTRY.get(scenario_id)
    if generator_class is None:
        raise KeyError(
            f"Scenario ID '{scenario_id}' not found in registry. "
            f"Available scenarios: {list(SCENARIO_REGISTRY.keys())}"
        )
    return generator_class(spark)


def run_scenario(scenario_id: str, spark: SparkSession, **kwargs) -> List[DataFrame]:
    """
    Run a scenario by ID and return the generated DataFrame(s).

    Args:
        scenario_id (str): ID of the scenario to run.
        spark (SparkSession): Active Spark session.
        **kwargs: Additional arguments for the generate method.

    Returns:
        List[DataFrame]: Generated DataFrame(s).
    """
    generator = get_scenario_generator(scenario_id, spark)
    return generator.generate(**kwargs)
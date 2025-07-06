from py_spark_skew_solutions.scenarios.scenario_factory import register_scenario
from py_spark_skew_solutions.scenarios.scenario_a import ScenarioA
from py_spark_skew_solutions.scenarios.scenario_b import ScenarioB
from py_spark_skew_solutions.scenarios.scenario_c import ScenarioC
from py_spark_skew_solutions.scenarios.scenario_d1 import ScenarioD1
from py_spark_skew_solutions.scenarios.scenario_d2 import ScenarioD2
from py_spark_skew_solutions.scenarios.scenario_d3 import ScenarioD3
from py_spark_skew_solutions.scenarios.scenario_d7 import ScenarioD7

register_scenario("A", ScenarioA)
register_scenario("B", ScenarioB)
register_scenario("C", ScenarioC)
register_scenario("D1", ScenarioD1)
register_scenario("D2", ScenarioD2)
register_scenario("D3", ScenarioD3)
register_scenario("D7", ScenarioD7)
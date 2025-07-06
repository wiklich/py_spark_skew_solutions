"""
Package initialization module for all skew scenario detectors.

Registers all available detectors automatically.
"""

from py_spark_skew_solutions.detector.scenario_detector_factory import register_detector
from py_spark_skew_solutions.detector.scenario_a_detector import ScenarioADetector
from py_spark_skew_solutions.detector.scenario_b_detector import ScenarioBDetector
from py_spark_skew_solutions.detector.scenario_c_detector import ScenarioCDetector
from py_spark_skew_solutions.detector.scenario_d1_detector import ScenarioD1Detector
from py_spark_skew_solutions.detector.scenario_d2_detector import ScenarioD2Detector
from py_spark_skew_solutions.detector.scenario_d3_detector import ScenarioD3Detector
from py_spark_skew_solutions.detector.scenario_d7_detector import ScenarioD7Detector

# Register all detectors on package load
register_detector("A", ScenarioADetector)
register_detector("B", ScenarioBDetector)
register_detector("C", ScenarioCDetector)
register_detector("D1", ScenarioD1Detector)
register_detector("D2", ScenarioD2Detector)
register_detector("D3", ScenarioD3Detector)
register_detector("D7", ScenarioD7Detector)
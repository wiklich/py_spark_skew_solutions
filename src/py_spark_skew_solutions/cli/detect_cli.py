"""
CLI module for detecting skew patterns in synthetic datasets.
"""

import typer
import textwrap

from py_spark_skew_solutions.utils.spark_utils import create_or_get_spark_session
from py_spark_skew_solutions.detector.all_scenario_detector import AllScenarioDetector
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.scenarios.scenario_a import ScenarioA
from py_spark_skew_solutions.scenarios.scenario_b import ScenarioB
from py_spark_skew_solutions.scenarios.scenario_c import ScenarioC
from py_spark_skew_solutions.scenarios.scenario_d1 import ScenarioD1
from py_spark_skew_solutions.scenarios.scenario_d2 import ScenarioD2
from py_spark_skew_solutions.scenarios.scenario_d3 import ScenarioD3
from py_spark_skew_solutions.scenarios.scenario_d7 import ScenarioD7

# Rich help panel name
RICH_HELP_PANEL_TEXT = "Scenario Details"

# Initialize Typer CLI app
detect_app = typer.Typer(help="Commands to detect skewed datasets")


@detect_app.command(
    "ALL",
    epilog=textwrap.dedent(f"""\
        [b]Description[/b]: Runs all available skew detectors.\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help="Detect all known scenarios."
)
def detect_all(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_col: str = typer.Option("id", "--key_col", "-k", help="Key column used in skew detection"),
    country_col: str = typer.Option("country", "--country_col", "-cc", help="Country column for composite key"),
    product_col: str = typer.Option("product", "--product_col", "-pc", help="Product column for composite key"),
    time_col: str = typer.Option("ts", "--time_col", "-t", help="Timestamp column for temporal skew detection")
):
    """
    Run all available skew detectors on the provided dataset.

    Usage:
        python -m src.py_spark_skew_solutions.cli.detect_cli ALL --input_path /tmp/data --key_col id
    """
    spark = create_or_get_spark_session("Skew Detector - All Scenarios")
    spark.sparkContext.setLogLevel("WARN")
    df = spark.read.parquet(input_path)

    detector = AllScenarioDetector()
    result = detector.detect(df, key_cols=[key_col], country_col=country_col, product_col=product_col, timestamp_col=time_col)

    print("\n===== 🧪 SKEW DETECTION REPORT =====")
    for scenario_id, detected in sorted(result.items()):
        print(f"{scenario_id}: {'⚠️ Skew Detected' if detected else '✅ No Skew'}")

def detect_scenario(scenario_id: str, **kwargs):
    result = detect(scenario_id=scenario_id, **kwargs)
    print(f"Scenario {scenario_id}: {'⚠️ Skew Detected' if result else '✅ No Skew'}")


@detect_app.command(
    ScenarioA.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioA.name}\n
        [b]Description[/b]: {ScenarioA.description}\n
        [b]Skew Type[/b]: {ScenarioA.skew_type}\n
        [b]Example[/b]: {ScenarioA.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioA.id} Scenario - {ScenarioA.name}. Description: {ScenarioA.description}"
)
def detect_a(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_col: str = typer.Option("id", "--key_col", "-k", help="Key column for skew analysis")
):
    """
    Detects Scenario A: Many keys with few records per key.
    """
    detect_scenario(scenario_id=ScenarioA.id, input_path=input_path, key_cols=[key_col])

@detect_app.command(
    ScenarioB.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioB.name}\n
        [b]Description[/b]: {ScenarioB.description}\n
        [b]Skew Type[/b]: {ScenarioB.skew_type}\n
        [b]Example[/b]: {ScenarioB.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioB.id} Scenario - {ScenarioB.name}. Description: {ScenarioB.description}"
)
def detect_b(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_col: str = typer.Option("id", "--key_col", "-k", help="Key column for skew detection")
):
    """
    Detects Scenario B: Few keys with many records.
    """
    detect_scenario(scenario_id=ScenarioB.id, input_path=input_path, key_cols=[key_col])


@detect_app.command(
    ScenarioC.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioC.name}\n
        [b]Description[/b]: {ScenarioC.description}\n
        [b]Skew Type[/b]: {ScenarioC.skew_type}\n
        [b]Example[/b]: {ScenarioC.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioC.id} Scenario - {ScenarioC.name}. Description: {ScenarioC.description}"
)
def detect_c(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_col: str = typer.Option("id", "--key_col", "-k", help="Key column for skew detection")
):
    """
    Detects Scenario C: Mixed pattern of small and large partitions.
    """
    detect_scenario(scenario_id=ScenarioC.id, input_path=input_path, key_cols=[key_col])


@detect_app.command(
    ScenarioD1.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD1.name}\n
        [b]Description[/b]: {ScenarioD1.description}\n
        [b]Skew Type[/b]: {ScenarioD1.skew_type}\n
        [b]Example[/b]: {ScenarioD1.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioD1.id} Scenario - {ScenarioD1.name}. Description: {ScenarioD1.description}"
)
def detect_d1(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_cols: str = typer.Option("id", "--key_cols", "-k", help="Key column for skew detection")
):
    """
    Detects Scenario D1: High cardinality with uneven distribution.
    """
    detect_scenario(scenario_id=ScenarioD1.id, input_path=input_path, key_cols=[key_cols])


@detect_app.command(
    ScenarioD2.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD2.name}\n
        [b]Description[/b]: {ScenarioD2.description}\n
        [b]Skew Type[/b]: {ScenarioD2.skew_type}\n
        [b]Example[/b]: {ScenarioD2.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioD2.id} Scenario - {ScenarioD2.name}. Description: {ScenarioD2.description}"
)
def detect_d2(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    country_col: str = typer.Option("country", "--country_col", "-cc", help="Country column for composite key"),
    product_col: str = typer.Option("product", "--product_col", "-pc", help="Product column for composite key")
):
    """
    Detects Scenario D2: Skewed combinations of multi-dimensional keys.
    """
    detect_scenario(scenario_id=ScenarioD2.id, input_path=input_path, key_cols=[country_col, product_col])


@detect_app.command(
    ScenarioD3.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD3.name}\n
        [b]Description[/b]: {ScenarioD3.description}\n
        [b]Skew Type[/b]: {ScenarioD3.skew_type}\n
        [b]Example[/b]: {ScenarioD3.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioD3.id} Scenario - {ScenarioD3.name}. Description: {ScenarioD3.description}"
)
def detect_d3(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    time_col: str = typer.Option("ts", "--time_col", "-t", help="Timestamp column for temporal skew detection")
):
    """
    Detects Scenario D3: Temporal skew with hourly peaks.
    """
    detect_scenario(scenario_id=ScenarioD3.id, input_path=input_path, timestamp_col=time_col)


@detect_app.command(
    ScenarioD7.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD7.name}\n
        [b]Description[/b]: {ScenarioD7.description}\n
        [b]Skew Type[/b]: {ScenarioD7.skew_type}\n
        [b]Example[/b]: {ScenarioD7.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Detect {ScenarioD7.id} Scenario - {ScenarioD7.name}. Description: {ScenarioD7.description}"
)
def detect_d7(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset"),
    key_col: str = typer.Option("customer_id", "--key_col", "-k", help="Key column for join skew detection")
):
    """
    Detects Scenario D7: Join skew between dimension and fact tables.
    """
    detect_scenario(scenario_id=ScenarioD7.id, input_path=input_path, key_cols=[key_col])

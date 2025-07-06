"""
CLI module for running the demonstration of skew data scenarios.
"""

import typer
import textwrap
from py_spark_skew_solutions.scenarios.scenario_a import ScenarioA 
from py_spark_skew_solutions.scenarios.scenario_b import ScenarioB
from py_spark_skew_solutions.scenarios.scenario_c import ScenarioC
from py_spark_skew_solutions.scenarios.scenario_d1 import ScenarioD1
from py_spark_skew_solutions.scenarios.scenario_d2 import ScenarioD2
from py_spark_skew_solutions.scenarios.scenario_d3 import ScenarioD3
from py_spark_skew_solutions.scenarios.scenario_d7 import ScenarioD7
from py_spark_skew_solutions.scenarios.scenario_factory import get_scenario


# Rich help panel name
RICH_HELP_PANEL_TEXT = "Scenario Details"


run_app = typer.Typer(help="Commands to run the demonstration")


def run_command(
    scenario_id: str,
    aqe_enabled: bool,
    demonstrate_skew: bool,    
    shuffle_partitions: int,
    **kwargs
) -> None:
    """
    Execute the demonstration workflow for a given skew scenario.

    This function retrieves the appropriate scenario by ID and runs its demonstration logic
    using the provided parameters. It serves as a generic wrapper to execute any registered scenario.

    Args:
        scenario_id (str): Identifier of the scenario to run (e.g., "A", "D7").
        demonstrate_skew (bool): If True, simulate or showcase skew behavior. If False, apply mitigation strategies.
        aqe_enabled (bool): Whether to enable Spark's Adaptive Query Execution during execution.
        shuffle_partitions (int): Number of shuffle partitions to use if AQE is disabled.
        **kwargs: Additional parameters required for the specific demonstration (e.g., input_path, output_path).

    Returns:
        None

    Raises:
        KeyError: If the scenario_id is not registered in the scenario factory.
        Exception: If any error occurs during scenario setup or execution.
    """
    scenario = get_scenario(scenario_id=scenario_id, spark=None)
    scenario.run_demonstration(
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,
        shuffle_partitions=shuffle_partitions,
        **kwargs
    )

@run_app.command(
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
def run_demo_scenario_a(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Demonstrates Scenario A: Many keys with few records per key.
    """
    run_command(
        scenario_id=ScenarioA.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_b(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Demonstrates Scenario B: Few Keys with Many Records.
    """
    run_command(
        scenario_id=ScenarioB.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_c(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Detects Scenario C: Mix of many small keys and few dominant ones.
    """
    run_command(
        scenario_id=ScenarioC.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_d1(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Detects Scenario D1: High Cardinality with Uneven Distribution. Many keys but some are disproportionately frequent.
    """
    run_command(
        scenario_id=ScenarioD1.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_d2(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Detects Scenario D2: Skewed Multi-Dimensional Key Combinations. One key combination (e.g., country+product) dominates the dataset.
    """
    run_command(
        scenario_id=ScenarioD2.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_d3(
    input_path: str = typer.Option(..., "--input_path", "-i", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Detects Scenario D3: Temporal Skew with Hourly Peaks. Skewed distribution of records across time, with one hour dominating.
    """
    run_command(
        scenario_id=ScenarioD3.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path=input_path,
        output_path=output_path
    )


@run_app.command(
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
def run_demo_scenario_d7(
    input_path_transaction: str = typer.Option(..., "--input_path_transaction", "-it", help="Path to the Parquet dataset."),
    input_path_customer: str = typer.Option(..., "--input_path_customer", "-ic", help="Path to the Parquet dataset."),
    output_path: str = typer.Option(..., "--output_path", "-o", help="Path to the Parquet dataset."),
    demonstrate_skew: bool = typer.Option(False, "--demonstrate_skew/--no-demonstrate_skew", "-ds/-nds", help="Whether to demonstrate or mitigate skew"),
    aqe_enabled: bool = typer.Option(True, "--aqe/--no-aqe", "-aqe/-no-aqe", help="Enable or disable Adaptive Query Execution"),
    shuffle_partitions: int = typer.Option(3, "--shuffle_partitions", "-sp", help="Number of Shuffle Partitions.")
):
    """
    Detects Scenario D7: Join Skew. Most transactions belong to a few customer_ids, causing uneven task load.
    """   
    run_command(
        scenario_id=ScenarioD7.id,
        aqe_enabled=aqe_enabled,
        demonstrate_skew=demonstrate_skew,        
        shuffle_partitions=shuffle_partitions,
        input_path_transaction=input_path_transaction,
        input_path_customer=input_path_customer,
        output_path=output_path
    )
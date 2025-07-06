"""
CLI module for generating synthetic skewed datasets using scenario-based generators.
"""

import typer
import textwrap
from datetime import datetime
from typing import Optional, List

from py_spark_skew_solutions.utils.spark_utils import create_or_get_spark_session
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario

from py_spark_skew_solutions.scenarios.scenario_a import ScenarioA
from py_spark_skew_solutions.scenarios.scenario_b import ScenarioB
from py_spark_skew_solutions.scenarios.scenario_c import ScenarioC
from py_spark_skew_solutions.scenarios.scenario_d1 import ScenarioD1
from py_spark_skew_solutions.scenarios.scenario_d2 import ScenarioD2
from py_spark_skew_solutions.scenarios.scenario_d3 import ScenarioD3
from py_spark_skew_solutions.scenarios.scenario_d7 import ScenarioD7


# Rich help panel name
RICH_HELP_PANEL_TEXT = "Scenario Details"


generate_data_app = typer.Typer(help="Commands to generate synthetic skewed datasets")


def generate_and_save(scenario_id: str, output_path: str, **kwargs):
    """
    Generate data using the specified scenario and save to Parquet format.

    Args:
        scenario_id (str): Scenario identifier.
        output_path (str): Base path where data will be saved.
        **kwargs: Additional parameters passed to the generator.
    """
    spark = create_or_get_spark_session(f"Data Generator - Scenario: {scenario_id}")
    df = run_scenario(scenario_id=scenario_id, spark=spark, **kwargs)[0]
    df.write.mode("overwrite").parquet(output_path)


# -------------------
# Typer Commands
# -------------------


@generate_data_app.command(
    ScenarioA.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioA.name}\n
        [b]Description[/b]: {ScenarioA.description}\n
        [b]Skew Type[/b]: {ScenarioA.skew_type}\n
        [b]Example[/b]: {ScenarioA.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioA.id} Scenario - {ScenarioA.name}. Description: {ScenarioA.description}"
)
def scenario_a(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    n_keys: int = typer.Option(2_000_000, "--n_keys", "-k", help="Number of distinct keys to generate")
):
    generate_and_save("A", output_path=output_path, n_keys=n_keys)


@generate_data_app.command(
    ScenarioB.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioB.name}\n
        [b]Description[/b]: {ScenarioB.description}\n
        [b]Skew Type[/b]: {ScenarioB.skew_type}\n
        [b]Example[/b]: {ScenarioB.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioB.id} Scenario - {ScenarioB.name}. Description: {ScenarioB.description}"
)
def scenario_b(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    heavy_key: str = typer.Option("HOT", "--heavy_key", "-hk", help="Key with high frequency"),
    heavy_rows: int = typer.Option(2_000_000, "--heavy_rows", "-hr", help="Number of rows for the hot key"),
    other_keys: int = typer.Option(10_000, "--other_keys", "-ok", help="Number of additional small keys")
):
    generate_and_save("B", output_path=output_path, heavy_key=heavy_key, heavy_rows=heavy_rows, other_keys=other_keys)


@generate_data_app.command(
    ScenarioC.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioC.name}\n
        [b]Description[/b]: {ScenarioC.description}\n
        [b]Skew Type[/b]: {ScenarioC.skew_type}\n
        [b]Example[/b]: {ScenarioC.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioC.id} Scenario - {ScenarioC.name}. Description: {ScenarioC.description}"
)
def scenario_c(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    n_small_keys: int = typer.Option(2_000_000, "--n_small_keys", "-sk", help="Number of small keys"),
    heavy_key: str = typer.Option("HOT", "--heavy_key", "-hk", help="Hot key with many records"),
    heavy_rows: int = typer.Option(200_000, "--heavy_rows", "-hr", help="Number of rows for the hot key")
):
    generate_and_save("C", output_path=output_path, n_small_keys=n_small_keys, heavy_key=heavy_key, heavy_rows=heavy_rows)


@generate_data_app.command(
    ScenarioD1.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD1.name}\n
        [b]Description[/b]: {ScenarioD1.description}\n
        [b]Skew Type[/b]: {ScenarioD1.skew_type}\n
        [b]Example[/b]: {ScenarioD1.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioD1.id} Scenario - {ScenarioD1.name}. Description: {ScenarioD1.description}"
)
def scenario_d1(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    n_keys: int = typer.Option(1_000_000, "--n_keys", "-k", help="Total number of distinct keys"),
    heavy_key: str = typer.Option("VIP", "--heavy_key", "-hk", help="Key with more occurrences than others"),
    heavy_rows: int = typer.Option(100_000, "--heavy_rows", "-hr", help="Extra rows for the heavy key")
):
    generate_and_save("D1", output_path=output_path, n_keys=n_keys, heavy_key=heavy_key, heavy_rows=heavy_rows)


@generate_data_app.command(
    ScenarioD2.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD2.name}\n
        [b]Description[/b]: {ScenarioD2.description}\n
        [b]Skew Type[/b]: {ScenarioD2.skew_type}\n
        [b]Example[/b]: {ScenarioD2.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioD2.id} Scenario - {ScenarioD2.name}. Description: {ScenarioD2.description}"
)
def scenario_d2(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    countries: Optional[List[str]] = typer.Option(None, "--countries", "-c", help="List of country codes"),
    products: Optional[List[str]] = typer.Option(None, "--products", "-p", help="List of product codes"),
    heavy_combo_country: str = typer.Option("BR", "--heavy_combo_country", "-cc", help="Country part of the heavy composite key"),
    heavy_combo_product: str = typer.Option("X", "--heavy_combo_product", "-cp", help="Product part of the heavy composite key"),
    heavy_rows: int = typer.Option(1_600_000, "--heavy_rows", "-hr", help="Rows for the heavy composite key"),
    other_rows: int = typer.Option(400_000, "--other_rows", "-or", help="Rows for other combinations")
):
    generate_and_save(
        "D2",
        output_path=output_path,
        countries=countries,
        products=products,
        heavy_combo=(heavy_combo_country, heavy_combo_product),
        heavy_rows=heavy_rows,
        other_rows=other_rows
    )


@generate_data_app.command(
    ScenarioD3.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD3.name}\n
        [b]Description[/b]: {ScenarioD3.description}\n
        [b]Skew Type[/b]: {ScenarioD3.skew_type}\n
        [b]Example[/b]: {ScenarioD3.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioD3.id} Scenario - {ScenarioD3.name}. Description: {ScenarioD3.description}"
)
def scenario_d3(
    output_path: str = typer.Option(..., "--output_path", "-o", help="Parquet output path"),
    start_year: int = typer.Option(2025, "--start_year", "-y", help="Start year for timestamps"),
    start_month: int = typer.Option(1, "--start_month", "-m", help="Start month for timestamps"),
    start_day: int = typer.Option(1, "--start_day", "-d", help="Start day for timestamps"),
    days: int = typer.Option(1, "--days", "-dy", help="Number of days to span data over"),
    heavy_hour: int = typer.Option(9, "--heavy_hour", "-hh", help="Hour with most activity"),
    heavy_rows: int = typer.Option(1_200_000, "--heavy_rows", "-hr", help="Number of rows during the heavy hour"),
    other_rows: int = typer.Option(800_000, "--other_rows", "-or", help="Rows spread across other hours")
):
    start = datetime(start_year, start_month, start_day)
    generate_and_save("D3",
                      output_path=output_path,
                      start=start,
                      days=days,
                      heavy_hour=heavy_hour,
                      heavy_rows=heavy_rows,
                      other_rows=other_rows)


@generate_data_app.command(
    ScenarioD7.id,
    epilog=textwrap.dedent(f"""\
        [b]Scenario[/b]: {ScenarioD7.name}\n
        [b]Description[/b]: {ScenarioD7.description}\n
        [b]Skew Type[/b]: {ScenarioD7.skew_type}\n
        [b]Example[/b]: {ScenarioD7.example}\n
    """),
    rich_help_panel=RICH_HELP_PANEL_TEXT,
    help=f"Generate data for {ScenarioD7.id} Scenario - {ScenarioD7.name}. Description: {ScenarioD7.description}"
)
def scenario_d7(
    output_path_transaction: str = typer.Option(..., "--output_path_transaction", "-ot", help="Base path for Parquet output for transactions"),
    output_path_customer: str = typer.Option(..., "--output_path_customer", "-oc", help="Base path for Parquet output for customers"),
    num_customers: int = typer.Option(10_000, "--num_customers", "-nc", help="Number of unique customer IDs"),
    total_transactions: int = typer.Option(2_000_000, "--total_transactions", "-tt", help="Total transaction count"),
    skewed_ratio: float = typer.Option(0.75, "--skewed_ratio", "-sr", help="Ratio of skewed transactions (e.g., 0.75 = 75%)")
):
    spark = create_or_get_spark_session(f"Data Generator - Scenario: {ScenarioD7.id}")
    dfs = run_scenario(scenario_id=ScenarioD7.id, 
                       spark=spark, 
                       num_customers = num_customers,
                       total_transactions = total_transactions,
                       skewed_ratio = skewed_ratio
                       )
    df_customer = dfs[0]
    df_transaction = dfs[1]
    df_customer.write.mode("overwrite").parquet(output_path_customer)
    df_transaction.write.mode("overwrite").parquet(output_path_transaction)

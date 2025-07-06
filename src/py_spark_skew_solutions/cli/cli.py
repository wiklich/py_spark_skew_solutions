"""
CLI module for running the application.
"""

import typer
from py_spark_skew_solutions.cli.setting import setup_app
from py_spark_skew_solutions.cli.generate_data_cli import generate_data_app
from py_spark_skew_solutions.cli.detect_cli import detect_app
from py_spark_skew_solutions.cli.run_cli import run_app

app = typer.Typer(
    rich_markup_mode="rich",
    help="""
        [b][#00ff00]Script for demonstrating Spark Skew, it has features like:[/][/b]\n
        - [dim][#00ff00]Generate data for Skew demonstration[/][/dim]
        - [dim][#00ff00]Detect some scenarios of Data Skew[/][/dim]
        - [dim][#00ff00]Running, demonstrating and solving few scenarios of Data Skew by examples[/][/dim]

        [#d40b0b]Listenin' to The Devil's Blood - The Time Of No Time Evermore album ♪♬♩♫♭♩[/]
        [#d40b0b]Thanks S.L. (In Memoriam) and F.L. for this timeless album!!![/]

        [b][#1c0031]Author:[/][/b] ☠ ☠ ☠  [#00ff00]RafHellRaiser from Wik-Lich ha-K-Nitzotzot Maradot[/] ☠ ☠ ☠
    """,
)

app.add_typer(generate_data_app, name="generate_skew_data")
app.add_typer(detect_app, name="detect_skew_data")
app.add_typer(run_app, name="run_demo")
app.add_typer(setup_app, name="setup")

def main():
    """
    Entry point for direct execution via Python, CLI or PySpark (e.g., spark-submit).
    """
    app()


if __name__ == "__main__":
    main()
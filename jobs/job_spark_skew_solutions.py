"""
Entry point script for Spark submission.

This script is the main entry point when submitting the job via spark-submit.
It delegates control to the Typer-based CLI in py_spark_skew_solutions.cli.cli.
"""


import sys

from py_spark_skew_solutions.cli.cli import main as cli_main

if __name__ == "__main__":
    sys.argv = ["job_spark_skew_solutions.py"] + sys.argv[1:] 
    cli_main()
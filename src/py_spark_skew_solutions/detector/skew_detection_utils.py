"""
Utility functions for skew detection in PySpark DataFrames.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from typing import Dict, Any, List

# ---------------------------------------------------------------------------
# Default thresholds (tune for your workload)
# ---------------------------------------------------------------------------

DEFAULT_THRESHOLDS = {
    "distinct_ratio_high": 0.8,     # Scenario A: distinct_keys / total_rows ≥ 80%
    "small_group_max": 10,          # Scenario A: max rows per key ≤ 10
    "distinct_ratio_low": 0.0005,   # Scenario B: distinct_keys / total_rows ≤ 0.05%
    "dominant_key_ratio": 0.5,      # Scenario B & D7: max_key_rows / total_rows ≥ 50%
    "heavy_key_ratio": 0.05,        # Scenarios D1+: heavy key threshold (5% of total)
    "heavy_tail_factor": 100,       # Scenarios C & D4: max/avg ≥ 100 ⇒ heavy tail
    "temporal_bucket": "hour"       # Scenario D3: bucket size for temporal skew
}

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _group_counts(df: DataFrame, key_cols: List[str]) -> DataFrame:
    """
    Group by key columns and count the number of rows per key.

    Args:
        df (DataFrame): Input DataFrame.
        key_cols (List[str]): Columns to group on.

    Returns:
        DataFrame: DataFrame with columns [key_cols..., count].
    """    
    return df.groupBy(*key_cols).count()


def _basic_metrics(df: DataFrame, key_cols: List[str]) -> Dict[str, Any]:
    """
    Compute basic distribution metrics for a DataFrame grouped by key columns.

    Args:
        df (DataFrame): Input DataFrame.
        key_cols (List[str]): Key columns used in skew analysis.

    Returns:
        Dict[str, Any]: Dictionary containing computed metrics:
            - total: Total number of rows
            - distinct: Number of distinct keys
            - max/min/avg/stddev: Distribution stats
            - p50/p90/p99: Approximate quantiles
            - counts_df: DataFrame with per-key row counts
    """
    total = df.count()
    if total == 0:
        return {"total": 0}

    counts_df = _group_counts(df, key_cols)

    stats_row = counts_df.agg(
        F.max("count").alias("max"),
        F.min("count").alias("min"),
        F.mean("count").alias("avg"),
        F.stddev("count").alias("stddev")
    ).first()

    quantiles = counts_df.approxQuantile("count", [0.5, 0.9, 0.99], 0.01)

    return {
        "total": total,
        "distinct": counts_df.count(),
        "max": stats_row["max"],
        "min": stats_row["min"],
        "avg": stats_row["avg"],
        "stddev": stats_row["stddev"] if stats_row["stddev"] is not None else 0,
        "p50": quantiles[0] if len(quantiles) > 0 else 0,
        "p90": quantiles[1] if len(quantiles) > 1 else 0,
        "p99": quantiles[2] if len(quantiles) > 2 else 0,
        "counts_df": counts_df
    }
"""
Scenario D7: Join Skew

Generates data where most transactions are associated with a small subset of customer IDs,
causing uneven task load during join operations.
"""

import logging
from pyspark.sql import DataFrame
from py_spark_skew_solutions.scenarios.scenario_base import ScenarioBase
from py_spark_skew_solutions.data_generator.scenario_data_generator_factory import run_scenario
from py_spark_skew_solutions.detector.skew_detector import detect
from py_spark_skew_solutions.demonstration.demonstration_factory import run_demonstration


# Configure logger for this module
logger = logging.getLogger(__name__)


class ScenarioD7(ScenarioBase):
    """
    Scenario D7 simulates a 'Join Skew' pattern, where the majority of transactions
    are associated with a small number of customer IDs. This leads to skewed partitions
    when joining or aggregating on customer_id, causing performance bottlenecks.

    Ideal for testing strategies such as salting, broadcast joins, or custom partitioning.
    """

    id = "D7"
    name = "Join Skew"
    description = "Most transactions belong to a few customer_ids, causing uneven task load."
    skew_type = "Join Skew"
    example = "'customer_id' where 1 percent of users account for 75 percent of all transactions."

    def generate_data(self, **kwargs) -> DataFrame:
        """
        Generate a skewed transaction DataFrame where a small percentage of customers
        account for the majority of transactions.

        Args:
            **kwargs: Optional parameters:
                - num_customers (int): Total number of distinct customer IDs. Default: 10,000.
                - total_transactions (int): Total number of transactions to generate. Default: 2,000,000.
                - skewed_ratio (float): Proportion of transactions assigned to top 1% of customers. Default: 0.75.

        Returns:
            DataFrame: Generated DataFrame with schema ['customer_id', 'transaction_id', 'amount']
        """
        num_customers = kwargs.get("num_customers", 10_000)
        total_transactions = kwargs.get("total_transactions", 2_000_000)
        skewed_ratio = kwargs.get("skewed_ratio", 0.75)

        logger.info(
            "Generating join-skewed DataFrame with parameters: "
            f"num_customers={num_customers}, total_transactions={total_transactions}, "
            f"skewed_ratio={skewed_ratio:.2f}"
        )

        df = run_scenario(
            scenario_id=self.id,
            spark=self.spark,
            num_customers=num_customers,
            total_transactions=total_transactions,
            skewed_ratio=skewed_ratio
        )

        logger.info("DataFrame successfully generated with schema: %s", df.schema.simpleString())
        return df

    def detect_skew(self, **kwargs) -> bool:
        """
        Detects skew by analyzing key distribution across partitions.

        Args:
            **kwargs: Optional parameters for skew detection.

        Returns:
            bool: Always returns False for this scenario (no significant skew expected).
        """
        result = detect(scenario_id=self.id, **kwargs)
        print(f"Scenario {self.id}: {'⚠️ Skew Detected' if result else '✅ No Skew'}")
        return result

    def run_demonstration(
        self,
        aqe_enabled: bool,
        demonstrate_skew: bool,
        shuffle_partitions: int = 200,
        **kwargs
    ) -> None:
        """
        Executes a full demonstration of Scenario D7.

        Args:
            aqe_enabled (bool): Whether Adaptive Query Execution is enabled during the demonstration.
            demonstrate_skew (bool): If True, demonstrates the impact of skew; if False, may skip it.
            shuffle_partitions (int): Number of shuffle partitions if AQE is disabled. Default: 200.
            **kwargs: Additional parameters for demonstration logic. Must include:
                - input_path (str): Path to read input data from.
                - output_path (str): Path to write results to.
        """
        logger.info("Starting demonstration for scenario '%s' (%s)", self.name, self.id)
        run_demonstration(
            scenario_id=self.id,
            aqe_enabled=aqe_enabled,
            demonstrate_skew=demonstrate_skew,
            shuffle_partitions=shuffle_partitions,
            **kwargs
        )
"""
Scenario D7: Generates a DataFrame with most transactions belong to a few customer_ids, causing uneven task load.
"""

import random
from typing import List
from pyspark.sql import DataFrame
from py_spark_skew_solutions.data_generator.base_scenario_data_generator import BaseScenarioDataGenerator
from py_spark_skew_solutions.utils.spark_utils import require_active_spark_session

BASE_ID = 1

class ScenarioD7DataGenerator(BaseScenarioDataGenerator):
    """
    Generates two DataFrames for a join skew scenario:
      - One small DataFrame of customers (unique customer_ids)
      - One large DataFrame of transactions, with most records concentrated in a few customer_ids

    Ideal for testing skewed joins between a dimension table and a fact table.
    """

    def generate(
        self,
        num_customers: int = 10_000,
        total_transactions: int = 2_000_000,
        skewed_ratio: float = 0.75
    ) -> List[DataFrame]:
        """
        Generate customer and transaction DataFrames.

        Parameters:
            num_customers (int): Total number of unique customer_ids.
            total_transactions (int): Total number of transaction records.
            skewed_ratio (float): Ratio of transactions assigned to skewed customers.

        Returns:
            List[DataFrame]: A list containing DataFrames:
                - customers_df: DataFrame with schema ['customer_id']
                - transactions_df: DataFrame with schema ['transaction_id', 'customer_id']
        """
        spark_session = require_active_spark_session()

        # Calculate number of skewed transactions
        skewed_count = int(total_transactions * skewed_ratio)

        # Define customer groups
        skewed_customers = list(range(BASE_ID, BASE_ID + 16))  # 16 hot customers
        normal_customers = list(range(BASE_ID + 16, BASE_ID + num_customers))

        # Generate customers DataFrame
        customers_data = [{"customer_id": i} for i in range(BASE_ID, BASE_ID + num_customers)]
        customers_df = spark_session.createDataFrame(customers_data)

        # Generate skewed transactions
        skewed_data = [(i, random.choice(skewed_customers)) for i in range(1, skewed_count + 1)]
        skewed_df = spark_session.createDataFrame(skewed_data, ["transaction_id", "customer_id"])

        # Generate normal transactions
        normal_data = [(i, random.choice(normal_customers)) for i in range(skewed_count + 1, total_transactions + 1)]
        normal_df = spark_session.createDataFrame(normal_data, ["transaction_id", "customer_id"])

        # Combine and repartition by customer_id for realistic distribution
        transactions_df = skewed_df.union(normal_df).repartition("customer_id")

        return [customers_df, transactions_df]